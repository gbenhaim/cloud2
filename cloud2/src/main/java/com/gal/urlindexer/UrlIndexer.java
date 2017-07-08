package com.gal.urlindexer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;


/**
 * Created by gbenhaim on 7/4/17.
 */
public class UrlIndexer {

    public static final String KEY = "gal";

    public static void main(String args[]) throws IOException {
        /*
        String url = "http://www.ynet.co.il/";
        UrlMapper urlMapper = new UrlMapper(url);
        urlMapper.calc();
        //System.out.println(urlMapper.getWordMap());
        //System.out.println(urlMapper.getUrlMap());
        //System.out.println(urlMapper.toJson());

        Gson gson = new Gson();

        HashMap<String, HashMap> currentMap = gson.fromJson(urlMapper.toJson(), new TypeToken<HashMap<String, HashMap>>(){}.getType());

        System.out.println(urlMapper.toCSV());
        */

        JobConf conf = new JobConf(UrlIndexer.class);
        conf.setJobName("urlIndexer");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String url = value.toString();
            UrlMapper urlMapper = new UrlMapper(url);
            urlMapper.calc();
            Text outText = new Text();
            outText.set(urlMapper.toJson());
            output.collect(new Text(KEY), outText);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
            Gson gson = new Gson();
            UrlMapper urlMapper = new UrlMapper(null);

            while (values.hasNext()) {
                HashMap<String, HashMap> currentMap = gson.fromJson(values.next().toString(), new TypeToken<HashMap<String, HashMap>>(){}.getType());
                HashMap<String, Double> currentUrlMap = currentMap.get(UrlMapper.URL_MAP);
                HashMap<String, Set<String>> currentWordMap = currentMap.get(UrlMapper.WORD_MAP);

                urlMapper.addUrlMap(currentUrlMap);
                urlMapper.addWordMap(currentWordMap);

            }
            output.collect(NullWritable.get(), new Text(urlMapper.toCSV()));
        }
    }


}
