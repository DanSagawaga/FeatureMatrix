import java.io.IOException;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

public class ConstructHTable {
      
   public static void main(String[] args) throws IOException {

	    final Logger slf4jLogger = LoggerFactory.getLogger(ConstructHTable.class);

      // Instantiating configuration class
	    Configuration hconf = HBaseConfiguration.create(conf);
	   // hConf.set("hbase.zookeeper.quorum", "hbaseZooKeeperQuorum");
	   // hConf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);

	   // HTable hTable = new HTable(hConf, tableName);	  System.out.println(conf.toString());

	   HBaseAdmin admin = new HBaseAdmin(conf);
	   

      
   //   HTable table = new HTable(conf, "people");
     // con.set("hbase.master", "http://quickstart.cloudera:60010");
     // Connection connection = ConnectionFactory.createConnection(con);

      // Instantiating HbaseAdmin class
     // Admin admin = connection.getAdmin();

      // Instantiating table descriptor class
  //    HTableDescriptor tableDescriptor = new
  //(TableName.valueOf("emp"));
//
      // Adding column families to table descriptor
   //   tableDescriptor.addFamily(new HColumnDescriptor("personal"));
  //    tableDescriptor.addFamily(new HColumnDescriptor("professional"));

      // Execute the table through admin
   ///   admin.createTable(tableDescriptor);
   //   System.out.println(" Table created ");
   }
}
