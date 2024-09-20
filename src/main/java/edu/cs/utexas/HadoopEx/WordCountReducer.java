package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, Text, Text, Text> {

   public void reduce(Text text, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
        int totalFlights = 0; //index 0
        float totalDelayDeparture = 0; //index 1
        
       
       for(Text v : values){
            String[] outputArr = v.toString().split(" ");
            if(outputArr.length == 2){
                try{
                    int flights = Integer.parseInt(outputArr[0]);
                    float delayTime = Float.parseFloat(outputArr[1]);

                    totalFlights += flights;
                    totalDelayDeparture += delayTime;
                } catch (Exception e){
                    continue;
                }
            }
       }
       String output = totalFlights + " " + totalDelayDeparture;
       context.write(text, new Text(output));
   }
}