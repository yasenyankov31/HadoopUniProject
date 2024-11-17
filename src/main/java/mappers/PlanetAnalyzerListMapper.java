
package mappers;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import uni.fmi.Utils.UtilHelper;

import org.apache.hadoop.mapred.JobConf;

public class PlanetAnalyzerListMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private String filterMethod;
	private String filterNumber;
	private Double filterOrbitalPeriod;
	private final static String METHOD_NAME = "method";

	@Override
	public void configure(JobConf job) {
		filterMethod = job.get("method") != null ? job.get("method").toString() : null;
		filterNumber = job.get("number");
		filterOrbitalPeriod = job.get("orbital_period") != null ? Double.parseDouble(job.get("orbital_period")) : null;
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] fields =  UtilHelper.filterFields(value.toString().split(","));

		if (fields.length < 6) {
			return; // Skip invalid rows
		}

		String method = fields[0];
		if(METHOD_NAME.equals(method))
			return;
		
		String number = fields[1];
		String orbitalPeriod = fields[2];

		boolean isMethodValid = filterMethod == null || method.contains(filterMethod);
		boolean isNumberValid = filterNumber == null || UtilHelper.toInteger(number) == UtilHelper.toInteger(filterNumber);
		boolean isOrbitalPeriodValid = filterOrbitalPeriod == null
				||  UtilHelper.toDouble(orbitalPeriod) >= filterOrbitalPeriod;

		if (isMethodValid && isNumberValid && isOrbitalPeriodValid) {
			output.collect(new Text(method), new Text(value.toString()));
		}
	}

}
