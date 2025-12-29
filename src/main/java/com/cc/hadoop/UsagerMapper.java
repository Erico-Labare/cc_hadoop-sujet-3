package com.cc.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UsagerMapper extends Mapper<LongWritable, Text, Text, Text> {

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

        // Num_Acc = index 0, grav = index 6
        if (fields.length > 6) {
            String numAcc = fields[0];
            String grav = fields[6].trim();

            if ("1".equals(grav)) {
                context.write(new Text(numAcc), new Text("U|" + grav));
            }
        }
    }
}

