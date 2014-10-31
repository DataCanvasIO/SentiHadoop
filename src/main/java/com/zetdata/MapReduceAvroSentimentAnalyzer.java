package com.zetdata;

/**
 * Created by xiaolin on 7/7/14.
 */

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import com.zetdata.avro.NlpResult;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceAvroSentimentAnalyzer extends Configured implements Tool {

    public static class SentimentAvroMapper
            extends Mapper<Object, Text, AvroKey<CharSequence>, AvroValue<NlpResult>>{

        private SentimentAnalyzer sentimentAnalyzer = new SentimentAnalyzer();

        @Override
        public void setup(Mapper<Object, Text, AvroKey<CharSequence>, AvroValue<NlpResult>>.Context context
        ) throws IOException {
            sentimentAnalyzer.Init();
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            for(NlpResult nlp_result : sentimentAnalyzer.processParagraph(value.toString())){
                AvroValue<NlpResult> sentimentResult = new AvroValue<NlpResult>(nlp_result);
                AvroKey<CharSequence> keyResult = new AvroKey<CharSequence>(key.toString());
                context.write(keyResult, sentimentResult);
            }
        }
    }

    Properties getParameters(String path) {
        Properties prop = new Properties();

        try {
            InputStream is = new FileInputStream(path);
            prop.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return prop;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration(getConf());

        Map<String, String> envs = System.getenv();
        if (envs.containsKey("AWS_ACCESS_KEY_ID") && envs.containsKey("AWS_SECRET_ACCESS_KEY")) {
            conf.set("fs.s3n.awsAccessKeyId", envs.get("AWS_ACCESS_KEY_ID"));
            conf.set("fs.s3n.awsSecretAccessKey", envs.get("AWS_SECRET_ACCESS_KEY"));
            conf.set("fs.s3.awsAccessKeyId", envs.get("AWS_ACCESS_KEY_ID"));
            conf.set("fs.s3.awsSecretAccessKey", envs.get("AWS_SECRET_ACCESS_KEY"));
        }

        // Override defaults by loading properties file
        if (envs.containsKey("PROPERTIES")) {
            System.out.println("Load properties from:" + envs.get("PROPERTIES"));
            Properties params = getParameters(envs.get("PROPERTIES"));
            for (Map.Entry<Object, Object> entry : params.entrySet()) {
                String propName = (String) entry.getKey();
                String propValue = (String) entry.getValue();
                conf.set(propName, propValue);
            }
        }

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MapReduceAvroSentimentAnalyzer <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "MapReduceAvroSentimentAnalyzer");
        job.setJarByClass(MapReduceAvroSentimentAnalyzer.class);
        job.setMapperClass(SentimentAvroMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        // job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, NlpResult.getClassSchema());

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new MapReduceAvroSentimentAnalyzer(), args);
        System.exit(ret);
    }
}
