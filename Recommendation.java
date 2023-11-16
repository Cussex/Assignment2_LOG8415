import java.io.IOException;
import java.util.*;	

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Recommendation {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            // Your map function code here
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
       
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            // Your reduce function code here
        }
    }

    public static void main(String[] args) {
        // Your main function code here
    }
}
