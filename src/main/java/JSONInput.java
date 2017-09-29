import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.Date;

public class JSONInput {
    public static class JSONMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            String[] elevation = str[8].split(":");  //Elevation:XXXX
            context.write(new Text(elevation[1]), new Text(value.toString()));
            System.out.println(value.toString());
//            for(String tmp : str)
//                context.write(new Text(tmp), one);
        }
    }

    public static class JSONReducer extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Mapper.Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                context.write(new Text("Elevation:" + key), new Text("Record:" + val));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if (args.length != 2) {
            System.err.println("Usage: taskb <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "TaskB");
        job.setJarByClass(JSONInput.class);
        job.setMapperClass(JSONMapper.class);
        job.setMapOutputKeyClass(Text.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(JSONReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setNumReduceTasks(2);
//        job.setOutputValueClass(IntWritable.class);

        CustomFileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(CustomFileInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long start = new Date().getTime();
        boolean status = job.waitForCompletion(true);
        long end = new Date().getTime();
        System.out.println("Job took "+(end-start) + "milliseconds");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
