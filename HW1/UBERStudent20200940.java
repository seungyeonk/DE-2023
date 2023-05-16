import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;


public class UBERStudent20200940{

	public static class UBERStudent20200940Mapper extends Mapper<Object, Text, Text, Text>{
		public static String[] dayStr = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
		private Text keyText = new Text();
		private Text valueText = new Text();
		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] st = value.toString().split(",");
			
			String region = st[0];
			
			Date date = new Date(st[1]);
			String day = dayStr[date.getDay()];
			String vehicles = st[2];
			String trips = st[3];
			
			keyText.set(region+","+day);
			valueText.set(trips+","+vehicles);
			
			context.write(keyText, valueText);
		}
	}

	public static class UBERStudent20200940Reducer extends Reducer<Text,Text,Text,Text> 
	{
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int trips;
			int totalTrips = 0;
			int vehicles;
			int totlalVehicles = 0;
			

			for (Text val : values) 
			{
				String[] str = val.toString().split(",");
				trips = Integer.parseInt(str[0]);
				vehicles = Integer.parseInt(str[1]);
				totalTrips += trips;
				totlalVehicles += vehicles;
			}
			
			result.set(totalTrips+","+totlalVehicles);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: UBERStudent20200940 <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBERStudent20200940");
		job.setJarByClass(UBERStudent20200940.class);
		job.setMapperClass(UBERStudent20200940Mapper.class);
		job.setReducerClass(UBERStudent20200940Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
