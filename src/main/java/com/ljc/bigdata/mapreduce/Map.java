package com.ljc.bigdata.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ljc.bigdata.measures.Counters.ANALYTICS;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	/* The key is nothing but the offset of each line in the text file: LongWritable
	 * The value is each individual linew: Text
	 */
	private static final Log LOG = LogFactory.getLog(Map.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) {
		context.getCounter(ANALYTICS.NUM_MAPPERS).increment(1);
		try {
			String line = value.toString();
			StringTokenizer stringTokenizer = new StringTokenizer(line);
			while (stringTokenizer.hasMoreTokens()) {
				value.set(stringTokenizer.nextToken().replaceAll("[-|!?\"';+.^:,#]",""));
				// value.set(stringTokenizer.nextToken().replaceAll("[^\\w\\s]",""));
				context.write(value, new IntWritable(1));
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			LOG.error("Error en el mapeo :NumberFormatException: " + e.getMessage());
			e.printStackTrace();			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("Error en el mapeo :IOException: " + e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error("Error en el mapeo :InterruptedException: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
