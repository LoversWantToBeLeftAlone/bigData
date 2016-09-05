package MR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class rankSort {
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
       
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "rankSort");
        job.setJarByClass(rankSort.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(rankSortMap.class);
        job.setSortComparatorClass(DoubleWritableDecreasingComparator.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
    
    public static class DoubleWritableDecreasingComparator extends Comparator {
	    public int compare(DoubleWritable a,DoubleWritable b){
	        return -super.compare(a, b);
	        
	    }
	    @Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	        // TODO Auto-generated method stub
	        return -super.compare(b1, s1, l1, b2, s2, l2);
	    }
	}

    public static class rankSortMap extends Mapper<Object, Text, DoubleWritable, Text> {
        private Text outPage = new Text();
        private DoubleWritable outPr = new DoubleWritable();  
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String page = line[0];
            Double pr=Double.parseDouble(line[1]);
            this.outPage.set(page);
            this.outPr.set(pr);
            context.write(this.outPr, this.outPage);
        }
    }
}