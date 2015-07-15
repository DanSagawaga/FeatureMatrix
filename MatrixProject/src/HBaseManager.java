import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;



public class HBaseManager {

	private static Configuration conf = null;
	private static Connection connection = null;     
	private static HBaseAdmin admin = null;
	private static HTable htable = null;
	private static Put p = null;
	Filter filter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
			new BinaryComparator(Bytes.toBytes("ColumnTest1")));


	@SuppressWarnings("deprecation")

	public HBaseManager()throws IOException{

		conf = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(conf);//instantiates HBase configuration	    
		admin = (HBaseAdmin)connection.getAdmin();
	}


	public void getRows(String ColumnFamily)throws IOException{
		Scan scan = new Scan();
		scan.setFilter(new FirstKeyOnlyFilter());
		ResultScanner scanner = htable.getScanner(scan);
		for (Result result : scanner)
			System.out.println("Row Key: " + Bytes.toInt(result.getRow()));	 
		scanner.close();
	}
	/*	public boolean recordExists (int rowPar, String familyNamePar, String columnPar)throws IOException{
		Get get = new Get(Bytes.toBytes(rowPar));
		get.addFamily(Bytes.toBytes(familyNamePar));	   
		Result result1 = htable.get(get);
		return ( (result1.getValue(Bytes.toBytes(familyNamePar), Bytes.toBytes(columnPar))) != null);


	}
	 */
	public String[] getColumns(String rowPar, String ColumnFamily) throws IOException	{
		Get get = new Get(Bytes.toBytes(rowPar));
		//get.addFamily(Bytes.toBytes());	   
		Result r = htable.get(get); 

		NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(Bytes.toBytes(ColumnFamily));
		String[] Quantifers = new String[familyMap.size()];

		int counter = 0;
		for(byte[] bQunitifer : familyMap.keySet()) {
			Quantifers[counter++] = Bytes.toString(bQunitifer);
		}

		return Quantifers;
	}
	
	public void pickTable(String tableNamePar)throws IOException{
		if(admin.tableExists(tableNamePar))
			htable = new HTable(conf, TableName.valueOf(tableNamePar));
		else System.out.println("Table does not exist");
	}

	public  void createTable(String tableNamePar, String familyNamePar)throws IOException{
		//instantiates table descriptor class
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableNamePar));	    //creating column family descriptor
		// Adding column families to table descriptor
		tableDescriptor.addFamily(new HColumnDescriptor(familyNamePar));
		// Execute the table through admin
		if(!admin.tableExists(tableNamePar)){
			admin.createTable(tableDescriptor);
			System.out.println("Created table: " + tableNamePar + ", With Family: " + familyNamePar);  
		}
		else{
			System.out.println("Table: " + tableNamePar + " already exsits.");
		}
	}
	
	public  void deleteTable(String tableNamePar ) throws IOException{
		if(admin.tableExists(tableNamePar)){
			disableTable(tableNamePar);
			admin.deleteTable(tableNamePar);
			System.out.println("Table: " + tableNamePar + " deleted");
		}
	}
	
	public  void switchTable(String tableName)throws IOException{

		htable = new HTable(conf, TableName.valueOf(tableName));
	}

	public  void disableTable(String tableNamePar)throws IOException{

		boolean b = admin.isTableDisabled(tableNamePar);
		if(b){
			System.out.println("Table-- " + tableNamePar + " --is already disabled");
		}
		else{
			admin.disableTable(tableNamePar);
			System.out.println("Table-- "+ tableNamePar + " --was disabled");
		}

	}
	
	public  void listTables()throws IOException{
		HTableDescriptor[] tableDescriptor = admin.listTables();
		// printing all the table names.
		for (int i=0; i<tableDescriptor.length;i++ ){
			System.out.println(tableDescriptor[i].getNameAsString());
		}
	}

	public void putRecord(int rowPar, String familyPar, String columnPar, int valuePar) throws IOException{
		String rowParStr = ""+rowPar;
		p = new Put(Bytes.toBytes(rowParStr));
		// accepts column family name, qualifier/row name ,value
		p.addColumn(Bytes.toBytes(familyPar),Bytes.toBytes(columnPar), Bytes.toBytes(valuePar));
		htable.put(p);
		htable.flushCommits();
		System.out.println("Addition to Table: "+ htable.getName() + ", Added Row: "+ rowPar + ", Column: "+columnPar+ ", Value: " + valuePar);
	}

	public void putRecord(String rowPar, String familyPar, String columnPar, int valuePar) throws IOException{
		p = new Put(Bytes.toBytes(rowPar));
		// accepts column family name, qualifier/row name ,value
		p.addColumn(Bytes.toBytes(familyPar),Bytes.toBytes(columnPar), Bytes.toBytes(valuePar));
		htable.put(p);
		htable.flushCommits();
		System.out.println("Addition to Table: "+ htable.getName() + ", Added Row: "+ rowPar + ", Column: "+columnPar+ ", Value: " + valuePar);
	}

	public String getRecord(String rowPar, String familyNamePar, String columnPar)throws IOException{
		Get get = new Get(Bytes.toBytes(rowPar));
		get.addFamily(Bytes.toBytes(familyNamePar));	   
		Result result1 = htable.get(get);  	  
		return (Bytes.toString(result1.getValue(Bytes.toBytes(familyNamePar), Bytes.toBytes(columnPar))));
	}

	public  void scanColumn(String familyNamePar, String columnPar )throws IOException{

		Scan scan = new Scan();
		scan.setFilter(filter);
		//scan.addColumn(Bytes.toBytes(familyNamePar), Bytes.toBytes(columnPar));
		ResultScanner scanner = htable.getScanner(scan);
		for (Result result : scanner){
			//byte[] bytes = result.getValue(Bytes.toBytes(familyNamePar), Bytes.toBytes(columnPar));
			// System.out.println("Found row : " + result);
			System.out.println("Row: " + Bytes.toString(result.getRow())); //+ " Record: " + new String(bytes));
		}
		scanner.close();
	}

	public String getFamilyNames()throws IOException{
		return htable.getTableDescriptor().toStringCustomizedValues();
	}

	
	public Configuration getConfiguration() {
		// TODO Auto-generated method stub
		return conf;
	}

}
