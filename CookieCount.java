import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.lang.IndexOutOfBoundsException;

public class CookieCount {
    public static class MyMap
            extends Mapper<LongWritable, Text, Text, Text> {
            public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                String line = value.toString();

                StringTokenizer tokenizerCookie = new StringTokenizer(line, "\n");

                while (tokenizerCookie.hasMoreTokens()) {
                    StringTokenizer tokenizerLine = new StringTokenizer(tokenizerCookie.nextToken(), ",");

                    String cookie = tokenizerLine.nextToken();
                    String date = tokenizerLine.nextToken();

                    context.write(new Text(date), new Text(cookie));
                }
            }
    }

    public static class MyReduce
            extends Reducer<Text, Text, Text, Text> {
            public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

                Set<String> s = new HashSet<String>();
                int count = 0;

                Iterator<Text> it = values.iterator();

                while (it.hasNext()) {
                    String v = it.next().toString();
                    count++;
                    s.add(v);
                }
                context.write(key, new Text(s.size() + "\t" + count));
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "CookieCount");
        job.setJarByClass(CookieCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
