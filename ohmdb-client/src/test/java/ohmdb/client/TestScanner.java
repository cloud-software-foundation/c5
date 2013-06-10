package ohmdb.client;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestScanner {
  ByteString tableName = ByteString.copyFrom(Bytes.toBytes("tableName"));
  OhmTable table;

  public TestScanner() throws IOException, InterruptedException {
    table = new OhmTable(tableName);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    TestScanner testingUtil = new TestScanner();
    testingUtil.scan();
  }

  public void scan() throws IOException {
    int i = 0;
    Result result;

    ResultScanner scanner = table.getScanner(new Scan().setStartRow(new byte[]{0x00}));
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
    table.close();
  }
}