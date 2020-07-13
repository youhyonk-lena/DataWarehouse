import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AvgExp {

    public static class AvgExpMapper
            extends Mapper<Object, Text, Text, FloatWritable> {

        // 2: continent 7: life 8: gnp
        protected void map(Object start, Text lines, Context result)
                throws IOException, InterruptedException {

            String[] col = lines.toString().split(",");
            String continent = col[2].replace("'", "");

            if (Float.parseFloat(col[8].replace("'", "")) > 10000.00) {
                FloatWritable life = new FloatWritable(Float.parseFloat(col[7].replace("'", "")));
                result.write(new Text(continent), life);
            }
        }
    }

    public static class AvgExpReducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private FloatWritable result = new FloatWritable();

        public void reduce(Text continent, Iterable<FloatWritable> life, Context context)
                throws IOException, InterruptedException {

            float avg = 0;
            int size= 0;

            for (FloatWritable val : life) {
                avg += val.get();
                size++;
            }

            avg = avg / size;

            result.set(avg);

            if (size >= 5) {
                context.write(continent, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar ae.jar AvgExp.java <input-hw5> <output-hw5>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "inf551_hw5_avgExp");

        job.setJarByClass(AvgExp.class);
        job.setMapperClass(AvgExpMapper.class);
        job.setReducerClass(AvgExpReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
