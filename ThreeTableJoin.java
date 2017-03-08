import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ThreeTableJoin {

	public static class TableUDMapper extends Mapper<Object, Text, Text, Text> {
		//Identify as UserAge table
		private final static String id = "UD~";
		private String joinvals = new String();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itl = new StringTokenizer(value.toString(),"\n");
			while (itl.hasMoreTokens()) {
				String line = itl.nextToken();
				StringTokenizer itr = new StringTokenizer(line,"\t");
				String token = itr.nextToken();
				joinvals=id+itr.nextToken();
				context.write(new Text(token), new Text(joinvals));
			}
		}
	}
	
	public static class TableUGMapper extends Mapper<Object, Text, Text, Text> {
		//Identify as User gender table
		private final static String id = "UG~";
		private String joinvals = new String();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itl = new StringTokenizer(value.toString(),"\n");
			while (itl.hasMoreTokens()) {
				String line = itl.nextToken();
				StringTokenizer itr = new StringTokenizer(line,"\t");
				String token = itr.nextToken();
				joinvals=id+itr.nextToken();
				context.write(new Text(token), new Text(joinvals));
			}
		}
	}


	public static class TablePVMapper extends Mapper<Object, Text, Text, Text> {
		//Identify as Page View table
		private final static String id = "PV~";
		private String joinvals = new String();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itl = new StringTokenizer(value.toString(),"\n");
			while (itl.hasMoreTokens()) {
				String line = itl.nextToken();
				StringTokenizer itr = new StringTokenizer(line,"\t");
				String pg = itr.nextToken();
				String token = itr.nextToken();
				String ts = itr.nextToken();
				joinvals=id+pg+"~"+ts;
				context.write(new Text(token), new Text(joinvals));
			}
		}
	}
	
	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
				List<String> pvlist = new ArrayList<>();
				List<String> udlist = new ArrayList<>();
				List<String> uglist = new ArrayList<>();
				for(Text val1 : values){
					//Add the table entries in separate lists for each table
					StringTokenizer itr = new StringTokenizer(val1.toString(),"~");
					String check = itr.nextToken();
					if(check.equals("PV")){
						pvlist.add(itr.nextToken()+"\t"+itr.nextToken());
					}else if(check.equals("UD")){
						udlist.add(itr.nextToken());
					}else if(check.equals("UG")){
						uglist.add(itr.nextToken());
					}
				}
				//Loop through all entries to perform cross-product to get the join
				for (String z : pvlist) {
				    for(String x: udlist){
				    	for(String y: uglist){
					    	String[] tmp=z.split("\t");
					    	context.write(key, new Text(tmp[0]+"\t"+x+"\t"+tmp[1]+"\t"+y));
				    	}
				    }
				}
		}
		//Can override the cleanup method similar to two table join to display header on the top
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "three table join");
		job.setJarByClass(ThreeTableJoin.class);
		job.setMapperClass(TableUDMapper.class);
		//Not doing combine because we want the output from all mapper classes to be sorted as one rather than separately
		//job.setCombinerClass(JoinReducer.class);
		job.setReducerClass(JoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TableUDMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TablePVMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, TableUGMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}