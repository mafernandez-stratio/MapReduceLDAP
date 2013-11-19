
package org.redoop.mapreduceldap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 *
 * @author miguel
 */
public class Daemons {
        
    public static String removeSymbols(String str){
        String result = str; 
        char lastChar = result.charAt(str.length()-1);
        //System.out.println(lastChar+"?");
        while(!(((lastChar>='a')&&(lastChar<='z'))||((lastChar>='A')&&(lastChar<='Z')))){
            result = result.substring(0, result.length()-1);
            //System.out.println("new string="+result);
            lastChar = result.charAt(result.length()-1);
            //System.out.println(lastChar+"?");
        }
        return result;
    }
    
    public static String extractDaemon(String line){
        line = line.substring(37);
        int pos = line.indexOf("daemon:");
        if(pos > -1){
            line = line.substring(pos+8);
            return removeSymbols(line.substring(0, line.indexOf(" ")));
        }
        return null;
    }
    
    public static class Map extends MapReduceBase 
            implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();                

        @Override
        public void map(LongWritable key, 
                        Text value, 
                        OutputCollector<Text, IntWritable> output, 
                        Reporter reporter) throws IOException {
            String line = value.toString();
            String daemon = extractDaemon(line);            
            if(daemon!=null){
                word.set(daemon);
                output.collect(word, one);
            }
        /*
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
        */
        }
    }    
    
    public static class Reduce extends MapReduceBase 
            implements Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterator<IntWritable> values, 
                           OutputCollector<Text, IntWritable> output, 
                           Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }    
    
    public static void main(String[] args) throws IOException {                        
        
        if(args.length<2){
            File f = new File(args[0]);
            FileReader fr = new FileReader(f);
            BufferedReader reader = new BufferedReader(fr);
            String line = reader.readLine();
            while(line!=null){
                String daemon = extractDaemon(line);
                if(daemon!=null){
                    System.out.println(daemon);
                }
                line = reader.readLine();
            }
            System.exit(0);
        }
        
        JobConf conf = new JobConf(Daemons.class);
        conf.setJobName("daemonscount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
    
}
