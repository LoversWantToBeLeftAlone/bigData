package MR;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class preLPA {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "pre_LPA");
		job.setJarByClass(preLPA.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(preLpaMap.class);
		job.setReducerClass(preLpaReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);


	}

	public static class preLpaMap extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tuple = line.split("\t");
			String A = tuple[0];		
			String[] array = tuple[1].split("\\|");
			
			for (int i = 0; i < array.length; i++) {
				context.write(new Text(array[i].split(",")[0]), new Text(A));
			}
			context.write(new Text(A), new Text("#"+tuple[1]));
		}

	}

	public static class preLpaReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			String links = "";
			String label="";
			Map<String,Integer> map=new HashMap<>();
		
			for (Text value : values) {
				String tmp = value.toString();
				if (tmp.startsWith("#")) {
					links = tmp.substring(1);
					continue;
				}
				if(!map.containsKey(tmp))
					map.put(tmp, 0);
				map.put(tmp, map.get(tmp)+1);
					
			}
			int max=Integer.MIN_VALUE;
			for(String lb:map.keySet()){
				int tmp=map.get(lb);
				if(tmp>max){
					max=map.get(lb);
					label=lb;
				}
				if(tmp==max){
					Random rand=new Random();
					int i=rand.nextInt();
					if(i>=0)
						label=lb;
				}
			}
			context.write(key, new Text(label+"\t"+links));
		}
	}
}