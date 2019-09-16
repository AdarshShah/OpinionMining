package com.ldce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class YoutubeOpinionAnalysis {

    public static final HashMap<Text,FloatWritable> opinions=new HashMap<Text,FloatWritable>();
    public static final HashSet<Text> keys=new HashSet<Text>();

    static{
        try {
            InputStream videos = YoutubeOpinionAnalysis.class.getResourceAsStream("/videos.tsv");
            BufferedReader input = new BufferedReader(new InputStreamReader(videos));
            String in;
            while((in=input.readLine())!=null){
                StringTokenizer tokens = new StringTokenizer(in);
                if(tokens.hasMoreTokens())
                {
                    String key = tokens.nextToken();
                    String value = tokens.nextToken();
                    opinions.put(new Text(key),new FloatWritable(Float.parseFloat(value)));    
                    keys.add(new Text(key));
                }
            }
            input.close();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        
    }

    public static class Map extends Mapper<LongWritable,Text,Text,FloatWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
                {;
            try {
                String comment = value.toString();
                for(Text opinion:keys){
                    if(comment.contains(opinion.toString())){
                        context.write(opinion,opinions.get(opinion));
                    }
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }

    public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> value,
                Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
                    
                    float result = 0;
                    for (FloatWritable val : value) {
                        result+=val.get();
                    }
                    context.write(key, new FloatWritable(result));
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(conf, "WordCount");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        job.setMapperClass(Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setReducerClass(Reduce.class);
        job.setJarByClass(YoutubeOpinionAnalysis.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path output = new Path(args[1]);
        try {
            FileInputFormat.addInputPath(job, new Path(args[0]));
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        FileOutputFormat.setOutputPath(job, output);
        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}