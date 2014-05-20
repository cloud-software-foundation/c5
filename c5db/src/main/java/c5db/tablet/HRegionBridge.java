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

package c5db.tablet;

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
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.regionserver.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.MultiRowMutationProcessor;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Bridge between the (complex) HRegion and the rest of c5.
 * <p/>
 * Provides an abstraction and test point, and lessons in how to abstract
 * and extract HRegion functionality.
 */
public class HRegionBridge implements Region {
  private static final Logger LOG = LoggerFactory.getLogger(HRegionBridge.class);

  private HRegionInterface theRegion;

  public HRegionBridge(final HRegionInterface theRegion) {
    this.theRegion = theRegion;
  }

  @Override
  public boolean mutate(MutationProto mutation, Condition condition) throws IOException {
    final MutationProto.MutationType type = mutation.getMutateType();
    boolean success;
    switch (type) {
      case PUT:
        if (condition == null || condition.getRow() == null) {
          success = simplePut(mutation);
        } else {
          success = checkAndPut(mutation, condition);
        }
        break;
      case DELETE:
        if (condition == null || condition.getRow() == null) {
          success = simpleDelete(mutation);
        } else {
          success = checkAndDelete(mutation, condition);
        }
        break;
      default:
        throw new IOException("mutate supports atomic put and/or delete, not " + type.name());
    }
    return success;
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
      LOG.error(e.getLocalizedMessage());
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
      LOG.error(e.getLocalizedMessage());
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
  public RegionScanner getScanner(c5db.client.generated.Scan scan) throws IOException {
    return theRegion.getScanner(ReverseProtobufUtil.toScan(scan));
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
    byte[] rowToLock = null;
    Collection<Mutation> mutations = new ArrayList<>();
    List<ResultOrException> resultOrExceptions = new ArrayList<>();

    for (Action action : regionAction.getActionList()) {
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
        return new RegionActionResult(new ArrayList<>(), exception);
      } else if (!hasGet && !hasMutation) {
        String errorMsg = "We have neither mutations or a get, this is an invalid action";
        NameBytesPair exception = buildException(new IOException(errorMsg));
        return new RegionActionResult(new ArrayList<>(), exception);
      } else if (rowToLock == null && hasMutation) {
        rowToLock = action.getMutation().getRow().array();
      }
      if (hasMutation) {
        if (!Arrays.equals(rowToLock, action.getMutation().getRow().array())) {
          String errorMsg = "Attempting multi row atomic transaction";
          NameBytesPair exception = buildException(new IOException(errorMsg));
          return new RegionActionResult(new ArrayList<>(), exception);
        } else {
          switch (action.getMutation().getMutateType()) {
            case APPEND:
              return new RegionActionResult(new ArrayList<>(), buildException(new IOException("Append not supported")));
            case INCREMENT:
              return new RegionActionResult(new ArrayList<>(), buildException(new IOException("Increment not supported")));
            case PUT:
              try {
                mutations.add(ReverseProtobufUtil.toPut(action.getMutation()));
                resultOrExceptions.add(new ResultOrException(action.getIndex(), new Result(), null));
              } catch (IOException e) {
                NameBytesPair exception = buildException(e);
                return new RegionActionResult(resultOrExceptions, exception);
              }
              break;
            case DELETE:
              mutations.add(ReverseProtobufUtil.toDelete(action.getMutation()));
              resultOrExceptions.add(new ResultOrException(action.getIndex(), new Result(), null));
              break;
          }
        }
      }

    }
    MultiRowMutationProcessor proc = new MultiRowMutationProcessor(mutations, Arrays.asList(rowToLock));
    try {
      theRegion.processRowsWithLocks(proc);
    } catch (IOException e) {
      return new RegionActionResult(new ArrayList<>(), buildException(e));
    }


    for (Action action : regionAction.getActionList()) {
      Get get = action.getGet();
      if (get != null) {
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

  //private ResultOrException processAction(Action action)
  private NameBytesPair buildException(final Throwable t) {
    return new NameBytesPair(
        t.getClass().getName(),
        ByteString.copyFromUtf8(StringUtils.stringifyException(t)).asReadOnlyByteBuffer());
  }
}
