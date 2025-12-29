package com.cc.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class AccidentReducer extends Reducer<Text, Text, Text, IntWritable> {

    private Map<String, String> depLookup = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {

        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null) {
            throw new IOException("Aucun fichier trouvé dans le cache distribué");
        }

        for (URI uri : cacheFiles) {

            File file = new File(new Path(uri.getPath()).getName());

            if (file.getName().contains("v_departement")) {

                BufferedReader br = new BufferedReader(new FileReader(file));
                String line;
                boolean header = true;

                while ((line = br.readLine()) != null) {
                    if (header) {
                        header = false;
                        continue;
                    }

                    String[] fields = line.replace("\"", "").split(",");

                    // DEP = index 0, NCCENR = index 6
                    if (fields.length > 6) {
                        depLookup.put(fields[0], fields[6]);
                    }
                }
                br.close();
            }
        }
    }


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String dep = null;
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split("\\|");

            if ("C".equals(parts[0])) {
                dep = parts[1];
            } else if ("U".equals(parts[0])) {
                count++;
            }
        }

        if (dep != null && count > 0) {
            String depName = depLookup.get(dep);
            if (depName != null) {
                context.write(new Text(depName), new IntWritable(count));
            }
        }
    }
}
