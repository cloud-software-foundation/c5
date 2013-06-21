package ohmdb.client;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class Populator {
  static ByteString tableName = ByteString.copyFrom(Bytes.toBytes("tableName"));


  public Populator() throws IOException, InterruptedException {
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    OhmTable table = new OhmTable(tableName);
    long start = System.currentTimeMillis();
    compareToHBasePut(table,
        Bytes.toBytes("cf"),
        Bytes.toBytes("cq"),
        Bytes.toBytes("value"));
    long end = System.currentTimeMillis();
    System.out.println("time:" + (end - start));
  }

  public static void compareToHBasePut(final TableInterface table,
                                       final byte[] cf,
                                       final byte[] cq,
                                       final byte[] value) throws IOException, InterruptedException {
    try {
      ArrayList<Put> puts = new ArrayList<>();
      for (int j = 1; j != 30; j++) {
        puts.clear();
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
