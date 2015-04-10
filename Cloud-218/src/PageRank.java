
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
public class PageRank {

	//Given Links - 1.0,A,B,C,D
	//              ^   ^    B,C,D are outlinks.
	//				PR  URL  
	//					B(C,D),C(D)
	//					D(A)
	//		Map: out(B,1.00)
	//			 out(c,0.5)
	//			 out(D,0.5)
	//			 out(D,0.5)
	//			 out(A,0.5)
	//			 
	//		Reducer: A->1.00
	//				 B-> 1.00
	//				 C->1.5
	//				 D->2.0


	public static class Map extends Mapper<Text , Text,  Text, Double> {

		private final static IntWritable link_score=new IntWritable(); 
		private Text word = new Text();

		public void map(Text key, Text values, Context context) 
				throws IOException, InterruptedException {

			List<String> items = Arrays.asList(values.toString().split("\\s*,\\s*"));
			if (items.size() < 2) {
				return;
			}

			double pagescore = Double.parseDouble(items.get(0));
			String myurl=items.get(1);
			//context.write(items);
			int num_outlinks = items.size() - 2;
			if (num_outlinks <= 0 ) {
				return;
			}
			double out_score = pagescore / num_outlinks;
			for (int i = 2; i < items.size() ; i++ ) {
				context.write(new Text(items.get(i)), out_score);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Iterator<Double>, Text, Double> 
	{
		public void reduce(Text key, Iterator<Double> value, Context context) 
				throws IOException, InterruptedException {
			double page_sum = 0.0;
			while (value.hasNext()) {
				page_sum += value.next();
			}
			context.write(key, page_sum);
			//context.write(key,page_sum,outlinks);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "pagerank");
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