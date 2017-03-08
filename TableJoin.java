import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TableJoin {

	public static class TableUDMapper extends Mapper<Object, Text, Text, Text> {
		//Identify as User table
		private final static String id = "UD~";
		private String joinvals = new String();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//Iterate over each line
			StringTokenizer itl = new StringTokenizer(value.toString(),"\n");
			while (itl.hasMoreTokens()) {
				//Split each line into tokens on tab delimiter
				String line = itl.nextToken();
				StringTokenizer itr = new StringTokenizer(line,"\t");
				//Retrieve user id for key
				String token = itr.nextToken();
				//Concatenate table id and user age for value, the separator is ~
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
			//Iterate over each line
			StringTokenizer itl = new StringTokenizer(value.toString(),"\n");
			while (itl.hasMoreTokens()) {
				//Split each line into tokens on tab delimiter
				String line = itl.nextToken();
				StringTokenizer itr = new StringTokenizer(line,"\t");
				//Retrieve the page id
				String pg = itr.nextToken();
				//Retrieve the user id for key
				String token = itr.nextToken();
				//Retrieve the timestamp
				String ts = itr.nextToken();
				//Concatenate the table id with page id and timestamp. The separator is ~
				joinvals=id+pg+"~"+ts;
				context.write(new Text(token), new Text(joinvals));
			}
		}
	}
	
	public static class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {
		public static List<Text> hold= new ArrayList<>();
		public static Text head = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
				List<String> pvlist = new ArrayList<>();
				List<String> udlist = new ArrayList<>();
				for(Text val1 : values){
					StringTokenizer itr = new StringTokenizer(val1.toString(),"~");
					String check = itr.nextToken();
					//Store the PV table entries and UD table entries in separate lists
					if(check.equals("PV")){
						//Storing both page id and timestamp from PV 
						pvlist.add(itr.nextToken()+"\t"+itr.nextToken());
					}else if(check.equals("UD")){
						udlist.add(itr.nextToken());
					}
				}
				//Loop through both lists to perform cross product of values
				for (String z : pvlist) {
				    for(String x: udlist){
				    	String[] tmp=z.split("\t");
				    	//Identify the header record based on key value: user_id
				    	//Store the header entry in head list
				    	//Store the other entries in hold list
				    	if(!key.toString().equals("user_id")){
				    		hold.add(new Text(tmp[0]+"\t"+x+"\t"+tmp[1]));
				    	}else{
				    		head=new Text(tmp[0]+"\t"+x+"\t"+tmp[1]);
				    	}
				    }
				}
			}
		//Override the cleanup method in Reducer to display the header at the beginning
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {			
			//Write header
			context.write(NullWritable.get(), head);
			//Follow with all other table values
			for (Text t:hold) {
				context.write(NullWritable.get(), t);
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "table join");
		job.setJarByClass(TableJoin.class);
		job.setMapperClass(TableUDMapper.class);
		//Not doing combine because we want the output from all mapper classes to be sorted as one rather than separately
		//job.setCombinerClass(JoinReducer.class);
		job.setReducerClass(JoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TableUDMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TablePVMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}