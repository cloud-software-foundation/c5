package ohmdb.replication;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.LastSequenceId;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: alex
 * Date: 7/23/13
 * Time: 9:10 AM
 * To change this template use File | Settings | File Templates.
 */
public class HRegionFacade extends UnimplementedHRegionFacade
    implements Watcher,
    HBaseRPCErrorHandler,
    Runnable,
    RegionServerServices,
    AdminProtos.AdminService.BlockingInterface {

  private static final int PORT_NUMBER = 8080;
  private final InetSocketAddress initialIsa;
  private final RpcServer rpcServer;
  private final String rpcName;
  private final String ZK_STRING = "localhost:2181";
  ZooKeeper zooKeeper;
  Configuration c;


  /**
   * @return list of blocking services and their security info classes that this server supports
   */
  private List<RpcServer.BlockingServiceAndInterface> getServices() {
    List<RpcServer.BlockingServiceAndInterface> bssi = new ArrayList<>(1);
    bssi.add(new RpcServer.BlockingServiceAndInterface(
        AdminProtos.AdminService.newReflectiveBlockingService(this),
        AdminProtos.AdminService.BlockingInterface.class));
    return bssi;
  }

  public HRegionFacade(String rpcName) throws IOException {
    this.rpcName = rpcName;
    zooKeeper = new ZooKeeper(ZK_STRING, 300, this);
    c = HBaseConfiguration.create();

    initialIsa = new InetSocketAddress(PORT_NUMBER);
    this.rpcServer = new RpcServer(this, this.rpcName, getServices(),
        initialIsa,
        10,
        10,
        c,
        HConstants.QOS_THRESHOLD);

    this.rpcServer.setErrorHandler(this);
    this.rpcServer.start();

  }

  public void join(){
    this.join();
  }

  @Override
  public RpcServerInterface getRpcServer() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }


  @Override
  public Configuration getConfiguration() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void run() {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public AdminProtos.RollWALWriterResponse rollWALWriter(RpcController controller, AdminProtos.RollWALWriterRequest request) throws ServiceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public static void main(String args[]){
    try {
      HRegionFacade hr = new HRegionFacade("replication");
      System.out.println("starting logging");
      hr.join();
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

    @Override
    public void updateRegionFavoredNodesMapping(String encodedRegionName, List<HBaseProtos.ServerName> favoredNodes) {
        return;
    }

    @Override
    public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
        return new InetSocketAddress[0];  //To change body of implemented methods use File | Settings | File Templates.
    }


}
