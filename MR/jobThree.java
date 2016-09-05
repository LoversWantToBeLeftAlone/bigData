package MR;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class jobThree {

	public static class mapThree extends Mapper<Object, Text, Text, Text> {

		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString().split("\t")[0];
			String count = value.toString().split("\t")[1];
			String[] array = str.split(",");
			if (array.length == 2) {
				String s1 = array[0];
				String s2 = array[1];
				Text word = new Text(s2 + "," + count);
				context.write(new Text(s1), word);
			}
		}
	}

	public static class SumCombiner extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder out = new StringBuilder();
			int sum = 0;
			for (Text val : values) {
				sum += Integer.parseInt(val.toString().split(",")[1]);
				out.append(val.toString()+ "|");
			}
			out.append(sum);
			context.write(key, new Text(out.toString()));
		}
	}

	public static class reduceThree extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			StringBuilder value = new StringBuilder();
			for (Text val : values) {
				String[] a = val.toString().split("\\|");
				sum = Integer.parseInt(a[a.length - 1]);
				for (int i = 0; i < a.length - 1; i++) {
					String link = a[i].split(",")[0];
					int count = Integer.parseInt(a[i].split(",")[1]);
					String p = String.format("%.6f", (double) count / (double) sum);
					value.append(link + "," + p + "|");

				}

			}

			context.write(key, new Text(value.toString()));

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "job3");
		job.setJarByClass(jobThree.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(mapThree.class);
		job.setCombinerClass(SumCombiner.class);
		job.setReducerClass(reduceThree.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}