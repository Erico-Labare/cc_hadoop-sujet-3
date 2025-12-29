package com.cc.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CaractMapper extends Mapper<LongWritable, Text, Text, Text> {

    private boolean isHeader = true;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (isHeader) {
            isHeader = false;
            return;
        }

        String line = value.toString();
        String[] fields = line.replace("\"", "").split(";");

        // Num_Acc = index 0, dep = index 6
        if (fields.length > 6) {
            String numAcc = fields[0];
            String dep = fields[6];

            context.write(new Text(numAcc), new Text("C|" + dep));
        }
    }
}
