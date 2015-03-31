package map;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;

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
	 
	            //Get the TF
	            double tf = getTF(numSplit[0], numSplit[1]);	
	            //Get the idf
	            double idf = getIDF(numOfDocs, numOfDocsWithWord);		
	            //tf.idf = tf * idf. Format and store.
	            DecimalFormat formatter = new DecimalFormat("#0.000");
	            double tfIdf = tf * idf;
	            String tfIdfString = formatter.format(tfIdf);
	 
	            //write the context. Offline portion is done :) YAY
	            //TODO NEED TO SETUP OUTPUT TO GIVE DOCUMENT, KEY, IDF FORMAT. You already have everything.
	            //TODO Some sort of encryption :) Have fun with it!
	            context.write(new Text(key + "@" + document), new Text(tfIdfString));
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

	/*
	 * Returns the IDF of the given ints. IDF = log2(N/ni). N = # of docs.
	 * ni = # of docs with the word. If # of docs == # docs with the word
	 * (AKA every doc has the word, I decided to return 1.
	 */
	public double getIDF(int _numOfDocs, int _numOfDocsWithWord) {
		if (_numOfDocs == _numOfDocsWithWord){
			return (double)_numOfDocs / (double)_numOfDocsWithWord;
		}
		else {
			DecimalFormat formatter = new DecimalFormat("#0.000");
			double temp = (double)((Math.log((double)_numOfDocs / (double)_numOfDocsWithWord)) / Math.log(2));
            String idfString = formatter.format(temp);
			return Double.parseDouble(idfString);
		}
	}

	/*
	 * Returns the TF of the given strings. TF = f/MAX(f). f = freq of word in document.
	 * MAX(f) = max # of occurences in any document.
	 */
	public double getTF(String string, String string2) {
		return (Double)(Double.valueOf(string) / Double.valueOf(string2));
	}

	public boolean setNumDocs(int num_Docs) {
		numDocs = num_Docs;
		return (numDocs > 0);
	}
}
