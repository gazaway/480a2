package map;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FirstRunning {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		/*
		 * First mapping. Gives reducer <(word@file), 1>
		 */
		public void map(LongWritable key, Text inputBook, Context context) throws IOException, InterruptedException {
			//decided to use the filename in the key, easier than having to parse
			//the document for an ebook #
			String file = ((FileSplit)context.getInputSplit()).getPath().getName();
			//Switched to using a string pattern and string builder instead
			//of a string tokenizer. Worked easier while the tokenizer would hickup
			//sometimes (reason unknown).
			Pattern pattern = Pattern.compile("\\w+");
	        Matcher match = pattern.matcher(inputBook.toString());
			StringBuilder build = new StringBuilder();
			while (match.find()) {
				String temp = match.group().trim().toLowerCase();
				//Using as a delimiter. 
				build.append(temp + "!" + file);
				context.write(new Text(build.toString()), new IntWritable(1));
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		/*
		 *First Reducer. Writes <(word@file), n> where n is the occurrence value
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			//Just need to sum the the number of occurences together. I decided
			//to use val.get() so that I could use a combiner. But yeah, the
			//combiner kept messing up. Here's to hoping I don't run out of 
			//memory.
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	/*
	 * Generic driver class with one exception.
	 */
	@SuppressWarnings("deprecation")
	public int runFirstMap(String arg) throws Exception {
		Configuration conf = new Configuration();
		
		//This captures the number of files in a path :)
		//I then return this number so that I can use it in my tf-idf 
		//calculation. I thought it was pretty slick.
		Path tempPath = new Path(arg);
		FileSystem fileSys = tempPath.getFileSystem(conf);
		FileStatus[] fileStat = fileSys.listStatus(tempPath);
		int temp = fileStat.length;
		
		Job job = new Job(conf, "480a2");
		job.setJarByClass(FirstRunning.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//job.setCombinerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(arg));
		FileOutputFormat.setOutputPath(job, new Path("/temp/"));

		job.waitForCompletion(true);
		return temp;
	}

}