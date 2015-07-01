import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;


public class ConstructHTable {

	public static void main (String args[]) throws IOException{
		
		Configuration conf = HBaseConfiguration.create();
		HTable hTable = new HTable(conf, "-ROOT-");
		System.out.println("Table is: " + Bytes.toString(hTable.getTableName()));
		hTable.close();
	}
}
