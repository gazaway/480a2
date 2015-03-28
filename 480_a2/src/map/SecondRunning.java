package map;

import java.io.IOException;

import map.FirstRunning.Reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Map;
 

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondRunning {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	
		//Restructures the input
		public void map(LongWritable key, org.w3c.dom.Text value, Context context) throws IOException, InterruptedException {
		//split the key value pairs
			String[] keyVal = value.toString().split("\t");
			//The unique identifier added in last map/reduce
			String[] keyFile = keyVal[0].split("!");
			context.write(new Text(keyFile[1]), new Text(keyFile[0] + "!" + keyVal[1]));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			//create hashmaps of the value key pairs
			Map<String, Integer> temp = new HashMap<String, Integer>();
			for (Text val : values) {
				//The unique identifier added in last map/reduce
				String[] valKey = val.toString().split("!");
				temp.put(valKey[0], Integer.valueOf(valKey[1]));
				sum += Integer.parseInt(valKey[1]);
			}
			for (String s : temp.keySet()){
				context.write(new Text(s + "!" + key.toString()), new Text(temp.get(s)+ "@" + sum));
			}
		}
	}
	
	@SuppressWarnings("deprecation")
	public void runFirstMap(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "480a2");
		job.setJarByClass(SecondRunning.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}