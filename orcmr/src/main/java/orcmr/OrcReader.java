package orcmr;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * 
 * @author tripathi
 *
 */
public class OrcReader extends Configured  implements Tool{

	public static int A_ID=0;
	public static int B_ID=1;
	public static int C_ID=2;
	
	public static int D_ID=0;
	public static int E_ID=1;
	

	public static class MyMapper<K extends WritableComparable, V extends Writable> 
	extends MapReduceBase implements Mapper<K, OrcStruct, Text, IntWritable> {

		private StructObjectInspector oip; 
		private final OrcSerde serde = new OrcSerde();

		public void configure(JobConf job) {
			Properties table = new Properties();
			table.setProperty("columns", "a,b,c");
			table.setProperty("columns.types", "int,string,struct<d:int,e:string>");

			serde.initialize(job, table);

			try {
				oip = (StructObjectInspector) serde.getObjectInspector();
			} catch (SerDeException e) {
				e.printStackTrace();
			}
		}
		public void map(K key, OrcStruct val,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
						throws IOException {
			System.out.println(val);
			List<? extends StructField> fields =oip.getAllStructFieldRefs();

			StringObjectInspector bInspector =
					(StringObjectInspector) fields.get(B_ID).getFieldObjectInspector();
			String b = "garbage";
			try {
				b =  bInspector.getPrimitiveJavaObject(oip.getStructFieldData(serde.deserialize(val), fields.get(B_ID)));
			} catch (SerDeException e1) {
				e1.printStackTrace();
			}
			
			

			OrcStruct struct = null;
			try {
				struct = (OrcStruct) oip.getStructFieldData(serde.deserialize(val),fields.get(C_ID));
			} catch (SerDeException e1) {
				e1.printStackTrace();
			}
			StructObjectInspector cInspector = (StructObjectInspector) fields.get(C_ID).getFieldObjectInspector();
			int d =   ((IntWritable) cInspector.getStructFieldData(struct, fields.get(D_ID))).get();
			String e = cInspector.getStructFieldData(struct, fields.get(E_ID)).toString();
			
			output.collect(new Text(b), new IntWritable(1));
			output.collect(new Text(e), new IntWritable(1));
		}


	}


	public static class MyReducer<K extends WritableComparable, V extends Writable> 
	extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, 
				Reporter reporter)
						throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				String value = values.next().toString();
				sum += Integer.parseInt(value.toString());
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new OrcReader(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		//remove output directory
		Runtime r = Runtime.getRuntime();
		Process p = r.exec("rm -rf ./target/out1");
		p.waitFor();
		BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = "";

		while ((line = b.readLine()) != null) {
			System.out.println(line);
		}
		b.close();


		JobConf job = new JobConf(new Configuration(), OrcReader.class);

		// Specify various job-specific parameters     
		job.setJobName("myjob");
		job.set("mapreduce.framework.name","local");
		job.set("fs.default.name","file:///");
		job.set("log4j.logger.org.apache.hadoop","INFO");
		job.set("log4j.logger.org.apache.hadoop","INFO");

		//push down projection columns
		job.set("hive.io.file.readcolumn.ids","1,2");
		job.set("hive.io.file.read.all.columns","false");
		job.set("hive.io.file.readcolumn.names","b,c");

		FileInputFormat.setInputPaths(job, new Path("./src/main/resources/000000_0.orc"));
		FileOutputFormat.setOutputPath(job, new Path("./target/out1"));

		job.setMapperClass(OrcReader.MyMapper.class);
		job.setCombinerClass(OrcReader.MyReducer.class);
		job.setReducerClass(OrcReader.MyReducer.class);


		job.setInputFormat(OrcInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		JobClient.runJob(job);
		return 0;
	}

}