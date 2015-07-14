import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;



public class HBaseRunner  {
  public static void main(String[] args) {
try{
    if (args.length != 1) {
      System.out.printf(
          "\nNOT RIGHT AMOUNT OF ARGUMENTS\n");
      System.exit(-1);
    }
    
    Configuration conf = HBaseConfiguration.create();
	//admin.pickTable("DanTestTable");
    //admin.putRecord("IndexRowTest1","FeatureFamily", "EmptyFeature", 0);

   // Configuration conf = admin.getConfiguration();

    
    Job job = new Job(conf,"HBaseRunner");   
    
    job.setJarByClass(HBaseRunner.class);
    job.setJobName("HBaseRunner Job");
    job.setMapperClass(HBaseMapper.class);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "DanTestTable");
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Writable.class);
    job.setNumReduceTasks(0); 

    FileInputFormat.setInputPaths(job, new Path(args[0]));   
    


    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
    
}
catch (Exception e){
System.out.println("EXCEPTION CAUGHT HAHAHAHA: " + e.getMessage());	
}
    
  }
}
