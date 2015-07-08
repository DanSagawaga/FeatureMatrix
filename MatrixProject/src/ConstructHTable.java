import java.io.IOException;
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
	    
	   //tableName = new TableName(Bytes.toBytes("DanTable")); 
	    
	   htable = new HTable(conf, TableName.valueOf("DanTable"));
	   
	   p = new Put(Bytes.toBytes("extraRow0"));
	   p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("feature 0"), Bytes.toBytes("Value0"));
	   p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("designation"), Bytes.toBytes("manager"));
	   p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("salary"), Bytes.toBytes("50000"));
	   
	   htable.put(p);
	   
	   
	   
	//System.out.println(htable.toString());
	   htable.close();
	//   createHTable("DanTable","Feature Family);
	 //  addColumn("DanTable0", "feature 0");
	   // deleteTable("test");
	    listTables();

	    //enableTable("cars");
	  //  addColumn("DanTable0","contactDetails");
	    
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
	      admin.createTable(tableDescriptor);
	      System.out.println("Created table: " + tableNamePar + " With Family: " + familyNamePar);  
   }
   
   public static void addColumn(String tableNamePar, String columnName)throws IOException, MasterNotRunningException, InvalidFamilyOperationException{
	    
	     HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnName);
	  // Adding column family
			if(admin.tableExists(tableNamePar)){
	      admin.addColumn(tableNamePar, columnDescriptor);
	      System.out.println("Column: " + columnName + " was added to Tba");
			}
   }

	public static void deleteColumn(String tableNamePar, String columnName) throws IOException{
		if(admin.tableExists(tableNamePar))
		admin.deleteColumn(tableNamePar,columnName); 
		else{
			System.out.println("Table: " + tableNamePar + " not found");
		}

	}

}