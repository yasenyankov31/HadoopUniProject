package mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import uni.fmi.Utils.UtilHelper;

public class ExoplanetDistanceFilterMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, Text, IntWritable> {
	private Double minDistance;
	private Double maxDistance;
	private final static String DISTANCE_NAME = "distance";

	@Override
	public void configure(JobConf job) {
		minDistance = job.get("min") != null ? Double.parseDouble(job.get("min")) : null;
		maxDistance = job.get("max") != null ? Double.parseDouble(job.get("max")) : null;
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String[] fields =  UtilHelper.filterFields(value.toString().split(","));
		if (fields.length < 6) {
			return; // Skip invalid rows
		}

		String distance = fields[4];
		if(DISTANCE_NAME.equals(distance))
			return;
		
		String number = fields[1];

		boolean isValidMin = minDistance == null || minDistance < UtilHelper.toDouble(distance);
		boolean isValidMax = maxDistance == null || maxDistance >  UtilHelper.toDouble(distance);


		if (isValidMin && isValidMax) {
			output.collect(new Text("planetCounter"), new IntWritable(Integer.valueOf(number)));
		}
	}

}
