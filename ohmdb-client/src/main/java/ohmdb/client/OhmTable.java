package ohmdb.client;

import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import ohmdb.ProtobufUtil;
import ohmdb.RequestConverter;
import ohmdb.client.generated.ClientProtos;
import ohmdb.client.generated.HBaseProtos;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;


public class OhmTable extends OhmShim {
  private RequestHandler handler;
  private OhmConnectionManager ohmConnectionManager;
  private AtomicLong commandId = new AtomicLong(0);

  /**
   * OhmTable is the main entry points for clients of OhmDB
   *
   * @param tableName The name of the table to connect to.
   */
  public OhmTable(ByteString tableName) throws IOException, InterruptedException {
    super(tableName);

    ohmConnectionManager = OhmConnectionManager.INSTANCE;
    Channel channel =
        ohmConnectionManager
            .getOrCreateChannel("localhost", OhmConstants.TEST_PORT);
    handler = channel
        .pipeline()
        .get(RequestHandler.class);
  }

  @Override
  public Result get(final Get get) throws IOException {
    final SettableFuture<ClientProtos.Response> resultFuture
        = SettableFuture.create();
    final ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
    final ClientProtos.GetRequest getRequest =
        RequestConverter.buildGetRequest(getRegionName(),
            get,
            false);
    call.setGet(getRequest);
    call.setCommand(ClientProtos.Call.Command.GET);
    call.setCommandId(commandId.incrementAndGet());

    try {
      handler.call(call.build(), resultFuture);
      return ProtobufUtil.toResult(resultFuture.get().getGet().getResult());
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    final Collection<Result> results = new ArrayList<>();
    final ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
    ClientProtos.MultiGetRequest multiGet =
        RequestConverter.buildMultiGetRequest(getRegionName(), gets, false);
    call.setCommand(ClientProtos.Call.Command.MULTI_GET);
    call.setMultiGet(multiGet);
    final SettableFuture<ClientProtos.Response> resultFuture
        = SettableFuture.create();
    call.setCommandId(commandId.incrementAndGet());

    try {
      handler.call(call.build(), resultFuture);
      ClientProtos.MultiGetResponse response = resultFuture.get().getMultiGet();

      for (ClientProtos.Result rawResult : response.getResultList()) {
        results.add(ProtobufUtil.toResult(rawResult));
      }

      return results.toArray(new Result[results.size()]);
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean exists(final Get get) throws IOException {
    ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
    ClientProtos.GetRequest getRequest =
        RequestConverter.buildGetRequest(getRegionName(),
            get,
            true);
    call.setGet(getRequest);
    call.setCommand(ClientProtos.Call.Command.GET);
    final SettableFuture<ClientProtos.Response> resultFuture
        = SettableFuture.create();
    call.setCommandId(commandId.incrementAndGet());

    try {
      handler.call(call.build(), resultFuture);
      return resultFuture.get().getGet().getExists();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }


  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    if (scan.getStartRow() != null && scan.getStartRow().length > 0) {
      if (scan.getStopRow() != null && scan.getStopRow().length > 0) {
        if (Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0) {
          throw new IOException("StopRow needs to be greater than StartRow");
        }
      }
    }
    SettableFuture<Long> future = SettableFuture.create();
    HBaseProtos.RegionSpecifier regionSpecifier =
        HBaseProtos.RegionSpecifier.newBuilder()
            .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
            .setValue(ByteString.copyFromUtf8("value")).build();
    ClientProtos.ScanRequest scanRequest = ClientProtos.ScanRequest.newBuilder()
        .setScan(ProtobufUtil.toScan(scan))
        .setRegion(regionSpecifier).build();
    ClientProtos.Call call = ClientProtos.Call.newBuilder()
        .setCommand(ClientProtos.Call.Command.SCAN)
        .setCommandId(commandId.incrementAndGet())
        .setScan(scanRequest)
        .build();

    try {
      handler.call(call, future);
      Long scannerId = future.get();
      return new ClientScanner(scan, getTableName(), scannerId);

    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  @Override
  public Boolean[] exists(List<Get> gets) throws IOException {
    final Collection<Result> results = new ArrayList<>();
    ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
    final SettableFuture<ClientProtos.Response> resultFuture
        = SettableFuture.create();
    ClientProtos.MultiGetRequest multiGet =
        RequestConverter.buildMultiGetRequest(getRegionName(), gets, true);
    call.setCommand(ClientProtos.Call.Command.MULTI_GET);
    call.setMultiGet(multiGet);
    call.setCommandId(commandId.incrementAndGet());
    try {
      handler.call(call.build(), resultFuture);
      ClientProtos.MultiGetResponse response = resultFuture.get().getMultiGet();

      for (ClientProtos.Result rawResult : response.getResultList()) {
        results.add(ProtobufUtil.toResult(rawResult));
      }
      return results.toArray(new Boolean[results.size()]);
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }

  }

  @Override
  public void put(final Put put) throws IOException {
    ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
    ClientProtos.MutateRequest mutateRequest =
        RequestConverter.buildMutateRequest(getRegionName(), put);
    call.setMutate(mutateRequest);
    call.setCommand(ClientProtos.Call.Command.MUTATE);
    call.setCommandId(commandId.incrementAndGet());
    final SettableFuture<ClientProtos.Response> resultFuture
        = SettableFuture.create();

    try {
      handler.call(call.build(), resultFuture);
      resultFuture.get();

    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    for (Put put : puts) {
      put(put);
    }
  }

  @Override
  public void delete(Delete delete) throws IOException {
    ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
    ClientProtos.MutateRequest mutateRequest =
        RequestConverter.buildMutateRequest(getRegionName(), delete);
    call.setMutate(mutateRequest);
    call.setCommand(ClientProtos.Call.Command.MUTATE);
    call.setCommandId(commandId.incrementAndGet());
    final SettableFuture<ClientProtos.Response> resultFuture
        = SettableFuture.create();

    try {
      handler.call(call.build(), resultFuture);
      resultFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    for (Delete delete : deletes) {
      delete(delete);
    }
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
    try {
      ClientProtos.MultiRequest multiRequest =
          RequestConverter.buildMultiRequest(getRegionName(), rm);
      call.setMulti(multiRequest);
      call.setCommand(ClientProtos.Call.Command.MULTI);
      call.setCommandId(commandId.incrementAndGet());
      final SettableFuture<ClientProtos.Response> resultFuture
          = SettableFuture.create();

      handler.call(call.build(), resultFuture);
      resultFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    //Currently if not called then we can't close the jvm.
    try {
      ohmConnectionManager.close();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

}
