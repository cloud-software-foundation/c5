package ohmdb.client;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CompareToHBase {
  private static HTable hTable;
  private static ByteString tableName =
      ByteString.copyFrom(Bytes.toBytes("tableName"));
  private static Configuration conf;

  byte[] cf = Bytes.toBytes("cf");

  public CompareToHBase() throws IOException, InterruptedException {
    conf = HBaseConfiguration.create();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    CompareToHBase testingUtil = new CompareToHBase();
    hTable = new HTable(conf, tableName.toByteArray());
    testingUtil.compareToHBaseScan();
    hTable.close();
  }

  public void compareToHBaseScan() throws IOException, InterruptedException {
    OhmTable table = new OhmTable(tableName);

    long As, Ae, Bs, Be;
    int i = 0;
    Result result;
    ResultScanner scanner;

    int j = 0;
    Bs = System.currentTimeMillis();
     scanner = hTable.getScanner(cf);
    do {
      j++;
      if (j % 1024 == 0) {
        System.out.print("!");
        System.out.flush();
      }
      if (j % (1024 * 80) == 0) {
        System.out.println("");
      }
      result = scanner.next();
    } while (result != null);
    Be = System.currentTimeMillis();

    scanner.close();

    scanner = table.getScanner(cf);
    As = System.currentTimeMillis();
    do {
      i++;
      if (i % 1024 == 0) {
        System.out.print("#");
        System.out.flush();
      }
      if (i % (1024 * 80) == 0) {
        System.out.println("");
      }
      result = scanner.next();
    } while (result != null);
    scanner.close();
    Ae = System.currentTimeMillis();


    System.out.println("A:" + String.valueOf(Ae - As) + "i:" + i);
    System.out.println("B:" + String.valueOf(Be - Bs) + "j:" + j);
    table.close();
  }
}