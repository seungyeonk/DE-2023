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
	public String title;
	public double rating;

	public Movie(String _title, double _rating) {
		this.title = _title;
		this.rating = _rating;
	}

	public String toString() {
		return title + " " + rating;
	}
}	



public class IMDBStudent20200940
{
	public static class MovieComparator implements Comparator<Movie> 
	{
		public int compare(Movie x, Movie y) 
		{
			if (x.rating > y.rating) return 1;
			if (x.rating < y.rating) return -1;
			return 0;
		}
	}

	public static void insertMovie(PriorityQueue q, String title, double r, int topK)
	{
		Movie top = (Movie)q.peek();
		if (q.size() < topK || top.rating < r) {
			Movie movie = new Movie(title, r);
			q.add(movie);
			if(q.size() > topK) q.remove();
		}
	}
			
	public static class IMDBMapper extends Mapper<Object, Text, IntWritable, Text>
	{
		IntWritable id = new IntWritable();
		Text title = new Text();
		Text rating = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] tokens = value.toString().split("::");

			if (tokens.length == 3) { 
				if (tokens[2].contains("Fantasy"))
				{
					id.set(Integer.parseInt(tokens[0]));
					String tmp = tokens[1] + "::M";
					title.set(tmp);
					context.write(id, title);
				}
			} else if (tokens.length == 4) { 
				id.set(Integer.parseInt(tokens[1]));
				rating.set(tokens[2]);
				context.write(id, rating);
			}

		}
	}

	public static class IMDBReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> 
	{
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;
		
		Text rslt = new Text();
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			int count = 0;
			String title = "";
			
			for (Text val : values) 
			{
				if (val.toString().contains("::M")) {
					String[] tokens = val.toString().split("::");
					title = tokens[0];
				} else {
					sum += Integer.parseInt(val.toString());
					count++;
				}	
			}
			if (title != "") {
				double avg = 0;
				if (count != 0) {
					avg = sum / (double)count;
				}
				insertMovie(queue, title, avg, topK);
			}
			
		}

		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			while(queue.size() != 0) {
				Movie movie = (Movie)queue.remove();
				double tmp = Math.round(mv.rating * 10)/10.0;
				context.write(new Text(movie.title), new DoubleWritable(tmp));
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: IMDB <in> <out> <topK>");
			System.exit(2);
		}
		
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		
		Job job = new Job(conf, "IMDB");
		job.setJarByClass(IMDBStudent20200940.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		job.waitForCompletion(true);
	}
}
