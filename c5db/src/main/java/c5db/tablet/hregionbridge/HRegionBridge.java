/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package c5db.tablet.hregionbridge;

import c5db.client.generated.Action;
import c5db.client.generated.Condition;
import c5db.client.generated.Get;
import c5db.client.generated.MutationProto;
import c5db.client.generated.NameBytesPair;
import c5db.client.generated.RegionAction;
import c5db.client.generated.RegionActionResult;
import c5db.client.generated.Result;
import c5db.client.generated.ResultOrException;
import c5db.regionserver.ReverseProtobufUtil;
import c5db.tablet.Region;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.regionserver.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.MultiRowMutationProcessor;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Bridge between the (complex) HRegion and the rest of c5.
 * <p/>
 * Provides an abstraction and test point, and lessons in how to abstract
 * and extract HRegion functionality.
 */
public class HRegionBridge implements Region {
  private static final Logger LOG = LoggerFactory.getLogger(HRegionBridge.class);
  LinkedTransferQueue<Map.Entry<SettableFuture<Boolean>, MutationProto>> batchExecutor = new LinkedTransferQueue<>();
  private HRegionInterface theRegion;
  int processors = Runtime.getRuntime().availableProcessors();
  PoolFiberFactory poolFiberFactory = new PoolFiberFactory(Executors.newFixedThreadPool(processors * 2));
  Fiber batcher = poolFiberFactory.create();

  public HRegionBridge(final HRegionInterface theRegion) {
    this.theRegion = theRegion;
    batcher.start();
    batcher.scheduleAtFixedRate(() -> {
      long begin = System.currentTimeMillis();
      if (batchExecutor.size() == 0) {
        return;
      }
      ArrayList<Map.Entry<SettableFuture<Boolean>, MutationProto>> arrayList = new ArrayList<>(10000);
      batchExecutor.drainTo(arrayList, 10000);
      batchMutateHelper(arrayList);
      long time = System.currentTimeMillis() - begin;
      if (time > 100) {
        LOG.error("batchMutate took longer than 100ms: {} ms for {} entries", time, arrayList.size());
      }
    }, 0l, 1l, TimeUnit.MILLISECONDS);
  }

  private void batchMutateHelper(List<Map.Entry<SettableFuture<Boolean>, MutationProto>> message) {

    List<Put> puts = message.parallelStream()
        .map(entry -> {
          try {
            return ReverseProtobufUtil.toPut(entry.getValue());
          } catch (IOException e) {
            e.printStackTrace();
            return null;
          }
        })
        .collect(Collectors.toList());

    OperationStatus[] mutationResult = null;
    try {
      mutationResult = theRegion.batchMutate(puts.toArray(new Mutation[puts.size()]));
    } catch (IOException e) {
      crash(e);
    }

    Arrays.stream(mutationResult).parallel().forEach(operationStatus -> {
      switch (operationStatus.getOperationStatusCode()) {
        case SUCCESS:
          break;
        default:
          crash("error! writing", operationStatus);
          break;
      }
    });

    message.parallelStream().forEach(f -> f.getKey().set(true));
  }

  private void crash(String s, OperationStatus operationStatus) {
    LOG.error(s, operationStatus);
    System.exit(0);
  }

  private void crash(Throwable t) {
    LOG.error("crashing due to exception", t);
    System.exit(0);
  }

  @Override
  public ListenableFuture<Boolean> batchMutate(MutationProto mutateProto) throws IOException {
    SettableFuture<Boolean> future = SettableFuture.create();
    batchExecutor.put(new TreeMap.SimpleEntry<>(future, mutateProto));
    return future;
  }

  @Override
  public boolean mutate(MutationProto mutateProto, Condition condition) throws IOException {
    final MutationProto.MutationType type = mutateProto.getMutateType();
    switch (type) {
      case PUT:
        if (condition == null || condition.getRow() == null) {
          return simplePut(mutateProto);
        } else {
          return checkAndPut(mutateProto, condition);
        }
      case DELETE:
        if (condition == null || condition.getRow() == null) {
          return simpleDelete(mutateProto);
        } else {
          return checkAndDelete(mutateProto, condition);
        }
      default:
        throw new IOException("mutate supports atomic put and/or delete, not " + type.name());
    }
  }

  private boolean checkAndPut(MutationProto mutation, Condition condition) throws IOException {
    boolean success;
    final byte[] row = condition.getRow().array();
    final byte[] cf = condition.getFamily().array();
    final byte[] cq = condition.getQualifier().array();

    final CompareFilter.CompareOp compareOp = CompareFilter.CompareOp.valueOf(condition.getCompareType().name());
    final ByteArrayComparable comparator = ReverseProtobufUtil.toComparator(condition.getComparator());

    success = theRegion.checkAndMutate(row,
        cf,
        cq,
        compareOp,
        comparator,
        ReverseProtobufUtil.toPut(mutation),
        true);
    return success;
  }

  private boolean simplePut(MutationProto mutation) {
    try {
      theRegion.put(ReverseProtobufUtil.toPut(mutation));
    } catch (IOException e) {
      LOG.error("error in HRegionBridge#simplePut", e);
      return false;
    }
    return true;
  }


  private boolean checkAndDelete(MutationProto mutation, Condition condition) throws IOException {
    boolean success;
    final byte[] row = condition.getRow().array();
    final byte[] cf = condition.getFamily().array();
    final byte[] cq = condition.getQualifier().array();

    final CompareFilter.CompareOp compareOp = CompareFilter.CompareOp.valueOf(condition.getCompareType().name());
    final ByteArrayComparable comparator = ReverseProtobufUtil.toComparator(condition.getComparator());

    success = theRegion.checkAndMutate(row,
        cf,
        cq,
        compareOp,
        comparator,
        ReverseProtobufUtil.toDelete(mutation),
        true);
    return success;
  }

  private boolean simpleDelete(MutationProto mutation) {
    try {
      theRegion.delete(ReverseProtobufUtil.toDelete(mutation));
    } catch (IOException e) {
      LOG.error("error in HRegionBridge#simpleDelete", e);
      return false;
    }
    return true;
  }

  @Override
  public boolean exists(Get get) throws IOException {
    final org.apache.hadoop.hbase.client.Get serverGet = ReverseProtobufUtil.toGet(get);
    org.apache.hadoop.hbase.client.Result result = theRegion.get(serverGet);
    return result.getExists();
  }

  @Override
  public Result get(Get get) throws IOException {
    final org.apache.hadoop.hbase.client.Get serverGet = ReverseProtobufUtil.toGet(get);
    return ReverseProtobufUtil.toResult(theRegion.get(serverGet));
  }

  @Override
  public RegionScanner getScanner(c5db.client.generated.Scan scanIn) throws IOException {
    Scan scan = ReverseProtobufUtil.toScan(scanIn);
    return theRegion.getScanner(scan);
  }

  @Override
  public RegionActionResult processRegionAction(RegionAction regionAction) {
    RegionActionResult regionActionResult;
    if (regionAction.getAtomic()) {
      regionActionResult = processActionsAtomically(regionAction);
    } else {
      regionActionResult = processActionsInParallel(regionAction);
    }
    return regionActionResult;
  }

  private RegionActionResult processActionsAtomically(RegionAction regionAction) {
    ArrayList<byte[]> rowsToLock = new ArrayList<>();
    Collection<Mutation> mutations = new ArrayList<>();
    List<ResultOrException> resultOrExceptions = new ArrayList<>();

    for (Action action : regionAction.getActionList()) {
      boolean hasGet = false;
      boolean hasMutation = false;

      if (action.getGet() != null && action.getGet().getRow() != null) {
        hasGet = true;
      }

      if (action.getMutation() != null && action.getMutation().getRow() != null) {
        hasMutation = true;
      }

      if (hasGet && hasMutation) {
        String errorMsg = "We have mutations and a get, this is an invalid action";
        NameBytesPair exception = buildException(new IOException(errorMsg));
        return new RegionActionResult(new ArrayList<>(), exception);
      } else if (!hasGet && !hasMutation) {
        String errorMsg = "We have neither mutations or a get, this is an invalid action";
        NameBytesPair exception = buildException(new IOException(errorMsg));
        return new RegionActionResult(new ArrayList<>(), exception);
      }
      if (hasMutation) {
        switch (action.getMutation().getMutateType()) {
          case APPEND:
            return new RegionActionResult(new ArrayList<>(), buildException(new IOException("Append not supported")));
          case INCREMENT:
            return new RegionActionResult(new ArrayList<>(), buildException(new IOException("Increment not supported")));
          case PUT:
            try {

              rowsToLock.add(action.getMutation().getRow().array());
              mutations.add(ReverseProtobufUtil.toPut(action.getMutation()));
              resultOrExceptions.add(new ResultOrException(action.getIndex(), new Result(), null));
            } catch (IOException e) {
              NameBytesPair exception = buildException(e);
              return new RegionActionResult(resultOrExceptions, exception);
            }
            break;
          case DELETE:
            rowsToLock.add(action.getMutation().getRow().array());
            mutations.add(ReverseProtobufUtil.toDelete(action.getMutation()));
            resultOrExceptions.add(new ResultOrException(action.getIndex(), new Result(), null));
            break;
        }
      }
    }
    MultiRowMutationProcessor proc = new MultiRowMutationProcessor(mutations, rowsToLock);
    try {
      theRegion.processRowsWithLocks(proc);
    } catch (IOException e) {
      return new RegionActionResult(new ArrayList<>(), buildException(e));
    }

    for (Action action : regionAction.getActionList()) {
      Get get = action.getGet();
      if (get != null && get.getRow() != null) {
        try {
          Result result = get(get);
          resultOrExceptions.add(new ResultOrException(action.getIndex(), result, null));
        } catch (IOException e) {
          resultOrExceptions.add(new ResultOrException(action.getIndex(), null, buildException(e)));
        }
      }
    }

    return new RegionActionResult(resultOrExceptions, null);
  }

  private RegionActionResult processActionsInParallel(RegionAction regionAction) {
    ResultOrException[] actionMap = regionAction
        .getActionList()
        .parallelStream()
        .map(this::processAction)
        .toArray(ResultOrException[]::new);
    return new RegionActionResult(Arrays.asList(actionMap), null);
  }

  private ResultOrException processAction(Action action) {
    boolean hasGet = false;
    boolean hasMutation = false;

    if (action.getGet() != null) {
      hasGet = true;
    }

    if (action.getMutation() != null) {
      hasMutation = true;
    }

    if (hasGet && hasMutation) {
      String errorMsg = "We have mutations and a get, this is an invalid action";
      NameBytesPair exception = buildException(new IOException(errorMsg));
      return new ResultOrException(action.getIndex(), null, exception);
    } else if (hasGet) {
      Result result = null;
      NameBytesPair nameBytesPair = null;
      try {
        result = get(action.getGet());
      } catch (IOException e) {
        nameBytesPair = buildException(e);
      }
      return new ResultOrException(action.getIndex(), result, nameBytesPair);
    } else if (hasMutation) {
      Result result = null;
      NameBytesPair nameBytesPair = null;
      try {
        boolean processed = mutate(action.getMutation(), new Condition());
        if (!processed) {
          throw new IOException("Mutation not processed");
        } else {
          result = new Result();
        }
      } catch (IOException e) {
        nameBytesPair = buildException(e);
      }
      return new ResultOrException(action.getIndex(), result, nameBytesPair);
    } else {
      String errorMsg = "We have a blank action. Please supply get or mutate";
      NameBytesPair exception = buildException(new IOException(errorMsg));
      return new ResultOrException(action.getIndex(), null, exception);
    }
  }

  /**
   * @param t The exception to stringify.
   * @return NameValuePair of the exception name to stringified version os exception.
   */
  private NameBytesPair buildException(final Throwable t) {
    return new NameBytesPair(
        t.getClass().getName(),
        ByteBuffer.wrap(Bytes.toBytes(StringUtils.stringifyException(t))));
  }

  @Override
  public boolean rowInRange(byte[] row) {
    return true;
  }
}