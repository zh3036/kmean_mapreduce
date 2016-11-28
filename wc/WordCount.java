import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {
    
    // MAP CLASS 

    public static class Map extends MapReduceBase 
                    implements Mapper<LongWritable, Text, Text, IntWritable> {
        
        public void map(LongWritable key, Text value, 
            OutputCollector<Text, IntWritable> output, Reporter report) throws IOException{
                    
            // Map Logic: Get line at given offset (key is the offset), Emit (Word, 1) tuples
            
            String line = value.toString();
            Scanner scanner = new Scanner(line);
            
            while (scanner.hasNext()) {	
            String token = scanner.next();
            Text word = new Text();
            word.set(token);
            output.collect(word, new IntWritable(1));	
            }
        }		
    }
    
    // REDUCE CLASS 
    public static class Reduce extends MapReduceBase 
                    implements Reducer<Text, IntWritable, Text, LongWritable> {
        
        public void reduce ( Text key, Iterator<IntWritable> values, 
            OutputCollector<Text, LongWritable> output, Reporter report) throws IOException {
        
            // Reduce Logic: Count occurances,  Emit (Word, occurance) tuple
            
            int count = 0;
            while (values.hasNext())
                count += values.next().get();
            
            output.collect(key, new LongWritable(count));
        }		
    }
    
    public static void main (String args[]) throws Exception {
        
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("WordCount Example!");
        
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
    
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
         
        JobClient.runJob(conf);
    }
}
