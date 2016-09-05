package MR;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class pageRank {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "pageRank");
		job.setJarByClass(pageRank.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(pageRankMap.class);
		job.setReducerClass(pageRankReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

	public static class pageRankMap extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tuple = line.split("\t");
			String A = tuple[0];
			double pr = Double.parseDouble(tuple[1]);
			if (tuple.length > 2) {
				String[] array = tuple[2].split("\\|");

				for (int i = 0; i < array.length; i++) {
					String link = array[i].split(",")[0];
					double linkPr = Double.parseDouble(array[i].split(",")[1]);
					String prValue = A + "," + pr * linkPr;
					context.write(new Text(link), new Text(prValue));
				}

				context.write(new Text(A), new Text("#" + tuple[2]));
			}

		}
	}

	public static class pageRankReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String links = "";
			double pagerank = 0.0;
			Iterator var8 = values.iterator();

			for (Text value : values) {
				String tmp = value.toString();
				if (tmp.startsWith("#")) {
					links = "\t" + tmp.substring(tmp.indexOf("#") + 1);
					continue;
				}
				String[] tuple = tmp.split(",");
				if (tuple.length > 1) {
					pagerank += Double.parseDouble(tuple[1]);

				}
			}

			pagerank = (double)0.15 + 0.85 * pagerank;
			context.write(new Text(key), new Text(String.valueOf(pagerank) + links));
		}
	}
}