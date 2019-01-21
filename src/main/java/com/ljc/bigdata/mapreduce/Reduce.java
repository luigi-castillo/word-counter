package com.ljc.bigdata.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.ljc.bigdata.measures.Counters.ANALYTICS;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(Reduce.class);
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		context.getCounter(ANALYTICS.NUM_REDUCERS).increment(1);
	}
	
	public void reduce (Text key, Iterable<IntWritable> values, Context context) {
		context.getCounter(ANALYTICS.NUM_GRUPOS).increment(1);
		
		int sum = 0;
		for (IntWritable x : values) {
			sum += x.get();
		}
		try {
			context.write(key, new IntWritable(sum));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("Error en el mapeo :IOException: " + e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error("Error en el mapeo :InterruptedException: " + e.getMessage());
			e.printStackTrace();
		}
		context.getCounter(ANALYTICS.LINES_WRITTEN).increment(1);
	}
}
