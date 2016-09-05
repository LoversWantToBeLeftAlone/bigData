package MR;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.util.FilterModifWord;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class jobOne {
	public static class mapOne extends Mapper<Object, Text, Text, Text> {
		private Set<String> stopWords;
		private Path[] localFiles;

		public void setup(Context context) throws IOException {
			stopWords = new TreeSet<>();
			Configuration conf = context.getConfiguration();
			localFiles = DistributedCache.getLocalCacheFiles(conf);
			for (int i = 0; i < localFiles.length; i++) {
				BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
				String line = new String();
				while ((line = br.readLine()) != null) {
					StringTokenizer itr = new StringTokenizer(line);
					while (itr.hasMoreTokens()){
						String s=itr.nextToken();
						stopWords.add(s);
						UserDefineLibrary.insertWord(s, "name", 1000);
					}
				}
				br.close();
			}

		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringBuilder out = new StringBuilder();
			List<Term> list = FilterModifWord.modifResult(ToAnalysis.parse(line));
			for (Term t : list) {
				String s = t.getNatureStr();
				if (s.equals("name") || s.equals("nr")) {
					String name = t.getRealName();
					if (stopWords.contains(name)) {
						out.append(name);
						out.append(",");
					}
				}
			}
			context.write(new Text(out.toString()), new Text(""));
		}

	}

	public static class reduceOne extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new Path("/data/task2/people_name_list.txt").toUri(), conf);
		Job job = new Job(conf, "job1");
		job.setJarByClass(jobOne.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(mapOne.class);
		job.setReducerClass(reduceOne.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
