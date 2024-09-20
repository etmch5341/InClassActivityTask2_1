package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Text> {

	// Create output Text object to hold output
	private Text output = new Text();
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		// String line = value.toString();
		// StringTokenizer itr = new StringTokenizer(line, ",");
		// for(int i = 0; i < 4 && itr.hasMoreTokens(); i++){
		// 	itr.nextToken();
		// }
		// word.set(itr.nextToken());

		// for(int i = 0; i < 6 && itr.hasMoreTokens(); i++){
		// 	itr.nextToken();
		// }

		// float departureDelay = 0;
		// try{
		// 	departureDelay = Float.parseFloat(itr.nextToken());
		// } catch (Exception e){
		// 	return;
		// }

		// output.set("1 " + departureDelay);
		// context.write(word, output);


		String[] fields = value.toString().split(",");

        // Ensure the line has enough columns
        if (fields.length > 11) {
            String airport_name = fields[4];
            float departureDelay = 0;

            try {
                departureDelay = Float.parseFloat(fields[11]);
            } catch (Exception e) {
                departureDelay = 0;  
            }

            word.set(airport_name);
            output.set("1 " + departureDelay); 
            context.write(word, output);
        }
	}
}