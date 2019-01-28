//importing packages and libraries
package vuyyuru.map.reduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//MapReduce class
public class NYPD_task2_MapReduce{
	public static void main(String[] args) throws Exception{
		Configuration newConfiguration = new Configuration();
		Job job = new Job(newConfiguration, "NYPD Task2");
		job.setJarByClass(NYPD_task2_MapReduce.class);
		job.setMapperClass(MapperTask.class);
		job.setReducerClass(ReducerTask.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

//Mapper task
class MapperTask extends Mapper <LongWritable, Text, Text , IntWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String token=value.toString();
		String[] tokens=token.split(",");
		if(tokens[0]!="#DATE") {
			//1. Date on which maximum number of accidents took place.
			String key1 = "v1_" + tokens[0];
			context.write(new Text(key1.toString()), new IntWritable(1));
			
			//2. Borough with maximum count of accident fatality
			String key2 = "v2_" + tokens[1];
			int temp2 = Integer.parseInt(tokens[4]) + Integer.parseInt(tokens[6]) + Integer.parseInt(tokens[8]) + Integer.parseInt(tokens[10]);
			context.write(new Text(key2), new IntWritable(temp2));
			
			//3. Zip with maximum count of accident fatality
			String key3 = "v3_" + tokens[2];
			int temp3 = Integer.parseInt(tokens[4]) + Integer.parseInt(tokens[6]) + Integer.parseInt(tokens[8]) + Integer.parseInt(tokens[10]);
			context.write(new Text(key3), new IntWritable(temp3));
			
			//4. Which vehicle type is involved in maximum accidents
			String key4 = "v4_" + tokens[11];
			context.write(new Text(key4), new IntWritable(1));
			String[] year = tokens[0].split("/");
			
			//5. Year in which maximum Number Of Persons and Pedestrians Injured
			String key5 = "v5_" + year[2];
			int temp5 = Integer.parseInt(tokens[3]) + Integer.parseInt(tokens[5]);
			context.write(new Text(key5), new IntWritable(temp5));
			
			//6. Year in which maximum Number Of Persons and Pedestrians Killed
			String key6 = "v6_" + year[2];
			int temp6 = Integer.parseInt(tokens[4]) + Integer.parseInt(tokens[6]);
			context.write(new Text(key6), new IntWritable(temp6));
			
			//7. Year in which maximum Number Of Cyclist Injured and Killed (combined)
			String key7 = "v7_" + year[2];
			int temp7 = Integer.parseInt(tokens[7]) + Integer.parseInt(tokens[8]);
			context.write(new Text(key7), new IntWritable(temp7));
			
			//8. Year in which maximum Number Of Motorist Injured and Killed (combined)
			String key8 = "v8_" + year[2];
			int temp8 =  Integer.parseInt(tokens[9]) + Integer.parseInt(tokens[10]);
			context.write(new Text(key8), new IntWritable(temp8));
		}
	}
}

//Reducer task
class ReducerTask extends Reducer <Text, IntWritable, Text, IntWritable>{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		IntWritable intWritableNew   = new IntWritable();
		Text textNew = new Text();
		int total_value = 0;
		for (IntWritable val : values){
			total_value+=val.get();	
		}
		textNew.set(key);
		intWritableNew.set(total_value);
		context.write(textNew,intWritableNew);	
	}
}
