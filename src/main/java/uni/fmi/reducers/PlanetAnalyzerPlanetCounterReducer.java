
package uni.fmi.reducers;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PlanetAnalyzerPlanetCounterReducer extends MapReduceBase
		implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		float sum = 0;
		int count = 0;

		while (values.hasNext()) {
			sum += values.next().get();
			count++;
		}

		double average = BigDecimal.valueOf(count == 0 ? 0 : sum / count).setScale(5, RoundingMode.HALF_UP).doubleValue();
		output.collect(key, new DoubleWritable(average));
	}
}
