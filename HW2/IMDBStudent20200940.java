import java.io.IOException;
import java.util.*;
import java.lang.Math;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;


class Movie
{
	public String category;
	public double rating;

	public Movie(String _category, double _rating) {
		this.category = _category;
		this.rating = _rating;
	}

	public String toString() {
		return category + " " + rating;
	}
}	

public class IMDBStudent20200940 
{
	public static class RateComparator implements Comparator<YouTube> 
	{
		public int compare(YouTube x, YouTube y) 
		{
			if (x.rating > y.rating) return 1;
			if (x.rating < y.rating) return -1;
			return 0;
		}
	}
	
	public static void insertMovie(PriorityQueue q, String category, double r, int topK)
	{
	
		Movie top = (Movie)q.peek();
		if (q.size() < topK || top.rating < r) {
			Movie movie = new Movie(category, r);
			q.add(movie);
			if(q.size() > topK) q.remove();
			
		}
		
	}
			
	public static class MovieMapper extends Mapper<Object, Text, Text, DoubleWritable>
	{
	
		Text word = new Text();
		DoubleWritable rating = new DoubleWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] tokens = value.toString().split("\\|");
			word.set(tokens[3]);
			rating.set(Double.parseDouble(tokens[6]));
			context.write(word, rating);

		}
	}

	public static class MovieReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> 
	{
		private PriorityQueue<YouTube> queue;
		private Comparator<Movie> comp = new RateComparator();
		private int topK;
		

		Text rslt = new Text();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
		{
			double sum = 0.0;
			int count = 0;
			
			for (DoubleWritable val : values) 
			{
				sum += val.get();
				count++;	
			}
			
			double avg = 0;
			if (count != 0) {	
				avg = sum / (double)count;
			}

			insertMovie(queue, key.toString(), avg, topK);
			
		}

		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<YouTube>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			while(queue.size() != 0) {
				Movie movie = (Movie)queue.remove();
				context.write(new Text(movie.category), new DoubleWritable(movie.rating));
				
				
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();


		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: IMDBStudent20200940 <in> <out> <topK>");
			System.exit(2);
		}
		
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		
		Job job = new Job(conf, "IMDBStudent20200940");
		job.setJarByClass(IMDBStudent20200940.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		job.waitForCompletion(true);
	}
}
