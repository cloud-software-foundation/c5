package ohmdb.client;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

public class CompareToHBase2 {
  private static ByteString tableName =
      ByteString.copyFrom(Bytes.toBytes("tableName"));

  byte[] cf = Bytes.toBytes("cf");

  public CompareToHBase2() throws IOException, InterruptedException {
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    CompareToHBase2 testingUtil = new CompareToHBase2();
    testingUtil.compareToHBaseScan();
  }

  public void compareToHBaseScan() throws IOException, InterruptedException {
    OhmTable table = new OhmTable(tableName);

    Result result = null;
    ResultScanner scanner;

    scanner = table.getScanner(cf);
    byte [] previousRow = {};
    int counter = 0;
    do {
      if (result != null){
        previousRow = result.getRow();
      }
      result = scanner.next();

      if (Bytes.compareTo(result.getRow(), previousRow) < 1){
          System.out.println(counter);
          System.exit(1);
        }

    } while (result != null);
    table.close();
  }
}