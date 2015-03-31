package map;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ThirdRunning {
	//This was necessary for how I determined the number of docs in the input folder.
	int numDocs;
	
	public class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		/*
		 * Third mapping. Passes <word, (file=n/N)> to the reducer. n = number
		 * of occurrences in document. N = total words.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			 String[] firstSplit = value.toString().split("\t");
		     String[] keySplit = firstSplit[0].split("@");
		     context.write(new Text(keySplit[0]), new Text(keySplit[1] + "=" + firstSplit[1]));
		}
	}
	
	public class Reduce extends Reducer<Text, Text, Text, Text> {
		
		/*
		 * Outputs the BCV { word0 = tfidf, word1 = tfdif, ... , wordN = tfidf }
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//I passed this in earlier.
	        int numOfDocs = numDocs;
	        // total frequency of this word
	        int numOfDocsWithWord = 0;
	        HashMap<String, String> temp = new HashMap<String, String>();
	        for (Text val : values) {
	            String[] split = val.toString().split("=");
	            numOfDocsWithWord++;
	            temp.put(split[0], split[1]);
	        }
	        for (String document : temp.keySet()) {
	            String[] numSplit = temp.get(document).split("/");
	 
	            //Term frequency is the quocient of the number of terms in document and the total number of terms in doc
	            double tf = Double.valueOf(Double.valueOf(numSplit[0])
	                    / Double.valueOf(numSplit[1]));
	 
	            //interse document frequency quocient between the number of docs in corpus and number of docs the term appears
	            double idf = (double) numOfDocs / (double) numOfDocsWithWord;
	 
	            //given that log(10) = 0, just consider the term frequency in documents
	            double tfIdf = numOfDocs == numOfDocsWithWord ?
	                    tf : tf * Math.log10(idf);
	 
	            context.write(new Text(key + "@" + document), new Text("[" + numOfDocsWithWord + "/"
	                    + numOfDocs + " , " + numSplit[0] + "/"
	                    + numSplit[1] + " , " + DF.format(tfIdf) + "]"));
	        }
	    }
	}
	
	/*
	 * Generic driver for map/reduce job.
	 */
	@SuppressWarnings("deprecation")
	public void runThirdMap(String arg, int num_Docs) throws Exception {
		setNumDocs(num_Docs);
		Configuration conf = new Configuration();

		Job job = new Job(conf, "480a2");
		job.setJarByClass(ThirdRunning.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("/temp2/"));
		FileOutputFormat.setOutputPath(job, new Path(arg));

		job.waitForCompletion(true);
	}

	public boolean setNumDocs(int num_Docs) {
		numDocs = num_Docs;
		return (numDocs > 0);
	}
}
