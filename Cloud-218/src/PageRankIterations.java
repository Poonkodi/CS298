import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

/* In order to do multiple iterations, 
 * The Map input should be the same as the Reduce output.
 * This version tries to do that.
 * 
 */

@SuppressWarnings("deprecation")
public class PageRankIterations {
	/*
	 * Map input : 1.0\tURL\tOUTLINK1\tOUTLINK2 .....
	 *  
	 *  2 types of map outputs:
	 *  	page-rank outs : OUTLINK1 : score / n
	 *  					 OUTLINK2 : score / n
	 *  					 ...
	 *  		 			 OUTLINKn : score / n
	 * 
	 * 		url-outlinke output:
	 * 						 URL : URL\tOUTLINK1\tOUTLINK2..
	 * 
	 */
	public static class Map extends Mapper<Text , Text,  Text, Text> {

		public void map(Text key, Text values, Context context) 
				throws IOException, InterruptedException {

			List<String> items = Arrays.asList(values.toString().split("\t"));
			if (items.size() < 2) {
				return;
			}

			double pagescore = Double.parseDouble(items.get(0));

			String myurl=items.get(1);

			String outlinks = StringUtils.join("\t", items.subList(1, items.size()));
			context.write(new Text(myurl), new Text("LINKS\t"+ outlinks) );

			int num_outlinks = items.size() - 2;
			if (num_outlinks <= 0 ) {
				return;
			}
			double out_score = pagescore / num_outlinks;
			for (int i = 2; i < items.size() ; i++ ) {
				context.write(new Text(items.get(i)), new Text("SCORE\t" + out_score));
				//context.write(new Text(items.get(i)), out_score);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Iterator<Text>, Text, Text> 
	{
		public void reduce(Text key, Iterator<Text> value, Context context) 
				throws IOException, InterruptedException {
			String link_str = "";

			double page_sum = 0.0;
			while (value.hasNext()) {
				Text v = value.next();				
				String[] spl = v.toString().split("\t");
				if (spl.length != 2) continue;

				if (spl[0].equals("LINKS")) {
					link_str = spl[1];
				} else if (spl[0].equals("SCORE")) {
					double score = Double.parseDouble(spl[1]);
					page_sum += score;
				}
			}
			context.write(key, new Text(page_sum + "\t" + link_str));
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