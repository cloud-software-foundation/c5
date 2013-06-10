package ohmdb.client;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HBasePop {
  static ByteString tableName = ByteString.copyFrom(Bytes.toBytes("tableName"));
  static Configuration conf = HBaseConfiguration.create();
  static byte[] cf = Bytes.toBytes("cf");

  public static void main(String[] args) throws IOException, InterruptedException {
    HBasePop testingUtil = new HBasePop();
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName.toByteArray());
    hTableDescriptor.addFamily(new HColumnDescriptor(cf));
    admin.createTable(hTableDescriptor);

    long start = System.currentTimeMillis();
    compareToHBasePut();
    long end = System.currentTimeMillis();
    System.out.println("time:" + (end - start));
  }

  public static void compareToHBasePut() throws IOException, InterruptedException {
    byte[] cq = Bytes.toBytes("cq");
    byte[] value = Bytes.toBytes("value");
    HTable table = new HTable(conf, tableName.toByteArray());

    try {
      ArrayList<Put> puts = new ArrayList<>();
      for (int j = 1; j != 10; j++) {
        for (int i = 1; i != 1024 * 81; i++) {
          puts.add(new Put(Bytes.vintToBytes(i * j)).add(cf, cq, value));
        }

        int i = 0;

        for (Put put : puts) {
          i++;
          if (i % 1024 == 0) {
            System.out.print("#");
            System.out.flush();
          }
          if (i % (1024 * 80) == 0) {
            System.out.println("");
          }
          table.put(put);
        }
        puts.clear();
      }
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

}
