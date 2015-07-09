import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;


/*
 * Hbase data model
 * Data is stored in tables 
 * Tables contain rows
 * 	-rows are referenced by a uinque key
 * 		-key is an array of bytes, anything can be key, String,long, data structures
 * 
 * Rows are made of columns which are grouped in column families 
 * 	labeled as "family:column" ex. "user:first_name"
 * Data is stored in cells by Row x, column-family x column
 * 
 */

public class ConstructHTable {

	private static Configuration conf = null;
	private static Connection connection = null;     
	private static HBaseAdmin admin = null;
	private static HTable htable = null;
	private static Put p = null;


	@SuppressWarnings("deprecation")
	public static void main(String[] args)throws IOException{
		conf = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(conf);//instantiates HBase configuration	    
		admin = (HBaseAdmin)connection.getAdmin();
		htable = new HTable(conf, TableName.valueOf("DanTestTable"));

		//   createHTable("DanTestTable", "FeatureFamily");
		listTables();
		// deleteTable("DanTable");

		// putRecord("DocID2", "FeatureFamily", "Feature1","Record Dan2");
		// putRecord("DocID3", "FeatureFamily", "Feature1","Record Dan3");
		//  putRecord("DocID4", "FeatureFamily", "Feature1","Record Dan4");
		//scanColumn("FeatureFamily", "Feature1");

		//  System.out.println(getRecord("DocID2", "FeatureFamily", "Feature1"));
		//System.out.println(htable.getTableDescriptor().getFamily(Bytes.toBytes("FeatureFamily")).getValues().toString());
		//System.out.println(Bytes.toString(get.getRow()));


		//Result result1 = htable.get(get);  	  
		//return (Bytes.toString(result1.getValue(Bytes.toBytes(familyNamePar), Bytes.toBytes(columnPar))));


	}


	public static void scanColumn(String familyNamePar, String columnPar )throws IOException{

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(familyNamePar), Bytes.toBytes(columnPar));
		ResultScanner scanner = htable.getScanner(scan);
		for (Result result = scanner.next(); result != null; result = scanner.next()){
			byte[] bytes = result.getValue(Bytes.toBytes(familyNamePar), Bytes.toBytes(columnPar));
			// System.out.println("Found row : " + result);
			System.out.println("Row: " + Bytes.toString(result.getRow()) + " Record: " + new String(bytes));
		}
		scanner.close();
	}

	public static String getRecord(String rowPar, String familyNamePar, String columnPar)throws IOException{
		Get get = new Get(Bytes.toBytes(rowPar));
		get.addFamily(Bytes.toBytes(familyNamePar));	   
		Result result1 = htable.get(get);  	  
		return (Bytes.toString(result1.getValue(Bytes.toBytes(familyNamePar), Bytes.toBytes(columnPar))));
	}
	public static void putRecord(String rowPar, String familyPar, String columnPar, String valuePar) throws IOException{
		p = new Put(Bytes.toBytes(rowPar));
		// accepts column family name, qualifier/row name ,value
		p.addColumn(Bytes.toBytes(familyPar),Bytes.toBytes(columnPar), Bytes.toBytes(valuePar));
		htable.put(p);
		htable.flushCommits();
		System.out.println("Addition to Table: "+ htable.getName() + ", Added Row: "+ rowPar + ", Column: "+columnPar+ ", Value: " + valuePar);
	}

	public static void deleteTable(String tableNamePar ) throws IOException{
		if(admin.tableExists(tableNamePar)){
			disableTable(tableNamePar);
			admin.deleteTable(tableNamePar);
			System.out.println("Table: " + tableNamePar + " deleted");
		}
	}

	public static void disableTable(String tableNamePar)throws IOException{

		boolean b = admin.isTableDisabled(tableNamePar);
		if(b){
			System.out.println("Table-- " + tableNamePar + " --is already disabled");
		}
		else{
			admin.disableTable(tableNamePar);
			System.out.println("Table-- "+ tableNamePar + " --was disabled");
		}

	}

	public static void enableTable(String tableNamePar)throws IOException{	    
		boolean b = admin.isTableEnabled(tableNamePar);
		if(b){
			System.out.println("Table-- " + tableNamePar + " --is already enabled");
		}
		else{
			admin.enableTable(tableNamePar);
			System.out.println("Table-- "+ tableNamePar + " --was enabled");
		}
	}

	public static void listTables()throws IOException{
		HTableDescriptor[] tableDescriptor = admin.listTables();

		// printing all the table names.
		for (int i=0; i<tableDescriptor.length;i++ ){
			System.out.println(tableDescriptor[i].getNameAsString());
		}
	}

	public static void createHTable(String tableNamePar, String familyNamePar)throws IOException{

		//instantiates table descriptor class
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableNamePar));	    //creating column family descriptor

		// Adding column families to table descriptor
		tableDescriptor.addFamily(new HColumnDescriptor(familyNamePar));
		// Execute the table through admin
		if(!admin.tableExists(tableNamePar)){
			admin.createTable(tableDescriptor);
			System.out.println("Created table: " + tableNamePar + " With Family: " + familyNamePar);  
		}
		else{
			System.out.println("Table: " + tableNamePar + " already exsits.");
		}
	}

	public static void addColumn(String tableNamePar, String columnName)throws IOException, MasterNotRunningException, InvalidFamilyOperationException{

		HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnName);
		// Adding column family
		if(admin.tableExists(tableNamePar)){
			admin.addColumn(tableNamePar, columnDescriptor);
			System.out.println("Column: " + columnName + " was added to Table");
		}
		else{
			System.out.println("Table does not Exist");
		}
	}

	public static void deleteColumn(String tableNamePar, String columnName) throws IOException{
		if(admin.tableExists(tableNamePar))
			admin.deleteColumn(tableNamePar,columnName); 
		else{
			System.out.println("Table: " + tableNamePar + " not found");
		}

	}

	public static void shutdown()throws IOException{
		admin.shutdown();
	}
}