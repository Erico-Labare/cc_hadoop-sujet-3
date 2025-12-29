package com.cc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AccidentDriver {

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: AccidentDriver <caract> <usagers> <departements> <output>");
            System.exit(1);
        }

        String caractPath = args[1];
        String usagersPath = args[2];
        String departementsPath = args[3];
        String outputPath = args[4];

        //DEBUG
        System.out.println("Caractères = " + caractPath);
        System.out.println("Usagers = " + usagersPath);
        System.out.println("Départements = " + departementsPath);
        System.out.println("Output = " + outputPath);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Accidents graves par département");
        job.setJarByClass(AccidentDriver.class);

        job.addCacheFile(new Path(departementsPath).toUri());

        MultipleInputs.addInputPath(job, new Path(caractPath), TextInputFormat.class, CaractMapper.class);
        MultipleInputs.addInputPath(job, new Path(usagersPath), TextInputFormat.class, UsagerMapper.class);

        job.setReducerClass(AccidentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

