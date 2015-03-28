package map;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FirstRunning {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		public static boolean isInteger(String s) {
			try {
				Integer.parseInt(s);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		}

		public void map(LongWritable key, Text inputBook, Context context) throws IOException, InterruptedException {
			String file = ((FileSplit)context.getInputSplit()).getPath().getName();
			String bookStrings = inputBook.toString();
			StringTokenizer stringToke = new StringTokenizer(bookStrings);
			int cntr = 0;
			String cur;
			while (cntr < 2) {
				cur = stringToke.nextToken();
				if (cur.equals("***")) {
					cntr++;
				}
			}
			while (stringToke.hasMoreTokens()) {
				cur = stringToke.nextToken();
				cur = cur.toLowerCase();
				cur = cur.replaceAll("\\W", "");
				context.write(new Text(cur + "!" + file + '\t'), new IntWritable(1));
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	@SuppressWarnings("deprecation")
	public void runFirstMap(String arg) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "480a2");
		job.setJarByClass(FirstRunning.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(arg));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}