package map;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

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

public class FirstMap {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		private String date;
		IntWritable right = new IntWritable();
		Text left = new Text();
		String cur = "";
		String temp = "";
		String hashedNum = "";
		boolean haveDate = false;
		boolean haveBookNum = false;
		boolean haveStart = false;
		

		 public static boolean isInteger(String s) {
             try { 
                 Integer.parseInt(s); 
             } catch(NumberFormatException e) { 
                 return false; 
             }
             return true;
         }
		
		public void map(LongWritable key, Text inputBook, Context context)
				throws IOException, InterruptedException {

			String bookStrings = inputBook.toString();
			StringTokenizer lineToke = new StringTokenizer(bookStrings, "\r\n");
			while (lineToke.hasMoreTokens() && (!haveDate || !haveBookNum)) {
				cur = lineToke.nextToken();
				cur = cur.replaceAll("\\p{P}", "");
				if (cur.contains("EBook")) {
					StringTokenizer bookNumToke = new StringTokenizer(cur);
					while (bookNumToke.hasMoreTokens()) {
						temp = bookNumToke.nextToken();
						if (temp.contains("EBook")){
							temp = bookNumToke.nextToken();
							if (isInteger(temp)){
								hashedNum = temp;
								haveBookNum = true;
							}
						}
					}
				} 
				if (cur.contains("Release Date")){
					StringTokenizer dateToke = new StringTokenizer(cur);
					while (dateToke.hasMoreTokens() && !haveDate){
						temp = dateToke.nextToken();
						if ((temp.length() == 4) && (isInteger(temp)) && ((temp.charAt(0) == '1') || (temp.charAt(0) == '2'))) {
							date = temp;
							haveDate = true;
						}
					}
				}
			}
			while (lineToke.hasMoreTokens()){
				cur = lineToke.nextToken();
				cur = cur.replaceAll("\\p{P}", "");
				StringTokenizer bookToke = new StringTokenizer(cur);
				while (bookToke.hasMoreTokens()){
					temp = bookToke.nextToken();
					temp = temp.toLowerCase();
					if (temp != "") {
						if (isInteger(hashedNum)) {
							left.set(Integer.parseInt(hashedNum) + '\t'+ '\t' + temp + '\t');
						} else {
							left.set("Book: <unknown...>" + '\t' + '\t' + temp + '\t');
						}
						right.set(0);
						context.write(left, right);
					}
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			Set<IntWritable> bookSet = new HashSet<IntWritable>();
			for (IntWritable val : values) {
				bookSet.add(val);
				sum++;
			}
			String leftS = key.toString() + '\t' + sum + '\t';
			context.write(new Text(leftS), new IntWritable(0));
		}
	}

	@SuppressWarnings("deprecation")
	public void runFirstMap(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcount");
		job.setJarByClass(FirstMap.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}