import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import org.apache.hadoop.conf.Configuration;

public class ConstructHTable {
      
   public static void main(String[] args) throws IOException {

      // Instantiating configuration class
      Configuration con = HBaseConfiguration.create();
      Configuration conf = HBaseConfiguration.create();
      
      HTable table = new HTable(conf, "people");
     // con.set("hbase.master", "http://quickstart.cloudera:60010");
     // Connection connection = ConnectionFactory.createConnection(con);

      // Instantiating HbaseAdmin class
     // Admin admin = connection.getAdmin();

      // Instantiating table descriptor class
      HTableDescriptor tableDescriptor = new
      HTableDescriptor(TableName.valueOf("emp"));

      // Adding column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("personal"));
      tableDescriptor.addFamily(new HColumnDescriptor("professional"));

      // Execute the table through admin
   ///   admin.createTable(tableDescriptor);
      System.out.println(" Table created ");
   }
}
