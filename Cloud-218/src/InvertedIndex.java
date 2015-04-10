
import java.io.IOException;
import java.util.*;       

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

@SuppressWarnings("deprecation")
public class InvertedIndex {

	public static class Map extends Mapper<LongWritable, Text,  Text, Text> {
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			int position=0;
			while (tokenizer.hasMoreTokens()) {
				//Get the word the word in the document
				word.set(tokenizer.nextToken());
				//Get the document id
				InputSplit isplit = context.getInputSplit();
				String filename = ((FileSplit) isplit).getPath().getName();
				//Document_id:The position of the word in the document
				context.write(word, new Text(filename + ":" + position));
				position++;
			}
		}
	} 

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values, Context context) 
				throws IOException, InterruptedException {
			StringBuilder sb =new StringBuilder();
			sb.append(key+" : ");
			while (values.hasNext()) {
				sb.append(values.next()+" , ");
			}
			context.write(key, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "invertedindex");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}