
package mappers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import uni.fmi.Utils.UtilHelper;

public class PlanetAnalyzerPlanetCounterMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	private static final String MASS_NAME = "mass";
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		String[] fields = UtilHelper.filterFields(value.toString().split(","));
		if (fields.length < 6) {
			return; // Skip invalid rows
		}

		String mass = fields[3];
		if(MASS_NAME.equals(mass)) {
			return;
		}
		
		double massValue = 0;
		try {
			massValue = Double.parseDouble(mass);
		} catch (Exception e) {
			return;
		}
		output.collect(new Text("averageMass"), new DoubleWritable(massValue));
	}

}
