package map;

import java.io.IOException;
import java.util.HashMap;

import map.SecondRunning.Map;
import map.SecondRunning.Reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ThirdRunning {
	//This was necessary for how I determined the number of docs
	//in the input folder.
	int numDocs;
	
	public class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		/*
		 * Third mapping. 
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			 String[] firstSplit = value.toString().split("\t");
		     String[] keySplit = firstSplit[0].split("@");
		     context.write(new Text(keySplit[0]), new Text(keySplit[1] + "=" + firstSplit[1]));
		}
	}
	
	public class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        // get the number of documents indirectly from the file-system (stored in the job name on purpose)
	        int numberOfDocumentsInCorpus = numDocs;
	        // total frequency of this word
	        int numberOfDocumentsInCorpusWhereKeyAppears = 0;
	        HashMap<String, String> tempFrequencies = new HashMap<String, String>();
	        for (Text val : values) {
	            String[] documentAndFrequencies = val.toString().split("=");
	            numberOfDocumentsInCorpusWhereKeyAppears++;
	            tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
	        }
	        for (String document : tempFrequencies.keySet()) {
	            String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");
	 
	            //Term frequency is the quocient of the number of terms in document and the total number of terms in doc
	            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
	                    / Double.valueOf(wordFrequenceAndTotalWords[1]));
	 
	            //interse document frequency quocient between the number of docs in corpus and number of docs the term appears
	            double idf = (double) numberOfDocumentsInCorpus / (double) numberOfDocumentsInCorpusWhereKeyAppears;
	 
	            //given that log(10) = 0, just consider the term frequency in documents
	            double tfIdf = numberOfDocumentsInCorpus == numberOfDocumentsInCorpusWhereKeyAppears ?
	                    tf : tf * Math.log10(idf);
	 
	            context.write(new Text(key + "@" + document), new Text("[" + numberOfDocumentsInCorpusWhereKeyAppears + "/"
	                    + numberOfDocumentsInCorpus + " , " + wordFrequenceAndTotalWords[0] + "/"
	                    + wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) + "]"));
	        }
	    }
	}
	
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

	private void setNumDocs(int num_Docs) {
		numDocs = num_Docs;
	}
}
