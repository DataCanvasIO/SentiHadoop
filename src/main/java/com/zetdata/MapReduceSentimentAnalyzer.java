package com.zetdata;

/**
 * Created by xiaolin on 7/7/14.
 */

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.MessageDigest;

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

public class MapReduceSentimentAnalyzer extends Configured implements Tool {

    public static class SentimentMapper
            extends Mapper<Object, Text, Text, Text>{

        private SentimentAnalyzer sentimentAnalyzer = new SentimentAnalyzer();

        public static String md5Java(String message){
            String digest = null;
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] hash = md.digest(message.getBytes("UTF-8"));

                //converting byte array to Hexadecimal String
                StringBuilder sb = new StringBuilder(2*hash.length);
                for(byte b : hash){
                    sb.append(String.format("%02x", b&0xff));
                }

                digest = sb.toString();

            } catch (UnsupportedEncodingException ex) {
                // Logger.getLogger(StringReplace.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println("UnsupportedEncodingException");
            } catch (NoSuchAlgorithmException ex) {
                // Logger.getLogger(StringReplace.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println("NoSuchAlgorithmException");
            }
            return digest;
        }

        @Override
        public void setup(Mapper<Object, Text, Text, Text>.Context context
        ) throws IOException {
            sentimentAnalyzer.Init();
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            NlpResult sentimentResult = sentimentAnalyzer.processSentence(value.toString());

            String sentiStr = String.format("%s,%f,%f,%f,%f,%f",
                    sentimentResult.getSentiment(),
                    sentimentResult.getSentimentVector().get(0),
                    sentimentResult.getSentimentVector().get(1),
                    sentimentResult.getSentimentVector().get(2),
                    sentimentResult.getSentimentVector().get(3),
                    sentimentResult.getSentimentVector().get(4));

            context.write(new Text(md5Java(value.toString())), new Text(sentiStr));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration(getConf());
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MapReduceSentimentAnalyzer <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "MapReduceSentimentAnalyzer");
        job.setJarByClass(MapReduceSentimentAnalyzer.class);
        job.setMapperClass(SentimentMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        // job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new MapReduceSentimentAnalyzer(), args);
        System.exit(ret);
    }
}
