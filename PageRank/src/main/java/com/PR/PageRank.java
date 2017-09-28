package com.PR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by deepchheda on 2/21/17.
 */
public class PageRank {
    public static enum totalLinks{
        numberOfLinks,
        delta;
    }




    public static void main(String[] args) throws Exception {

        // pre-processing to get adjacency list
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Preprocessing");
        job.setJarByClass(com.PR.PageRank.class);
        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(ParseReducer.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"0"));
        job.waitForCompletion(true);

        long df = job.getCounters().findCounter(totalLinks.numberOfLinks).getValue();
        double dfd = (double) df;
        double del = 0.0;
        String deltavalue = "0.0";
        // stuff to go in loop
        // code for page rank
        for (int i = 1; i<=10; i++) {
            Configuration conf2 = new Configuration();
            conf2.set("InitialPR", String.valueOf(dfd));
            conf2.set("delta", deltavalue);
            Job job2 = Job.getInstance(conf2, "com.PR.PageRank");
            job2.setJarByClass(com.PR.PageRank.class);
            job2.setMapperClass(PageRankMapper.class);
            job2.setReducerClass(PageRankReducer.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]+String.valueOf(i-1)));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]+String.valueOf(i)));
            job2.waitForCompletion(true);
            del =(double) job2.getCounters().findCounter(totalLinks.delta).getValue();
            del = del/1000000000000000.0;
            deltavalue = String.valueOf(del);
            deltavalue = String.valueOf(del);
        }

        // finding top k
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Preprocessing");
        job3.setJarByClass(com.PR.PageRank.class);
        job3.setMapperClass(TopKMapper.class);
        job3.setReducerClass(TopKReducer.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[1]+"10"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        job3.waitForCompletion(true);




    }

    //mapper for parser
    public static class ParseMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            int delimLoc = line.indexOf(':');
            String pageName = line.substring(0, delimLoc);
            Text sol = null;
            try {
                sol = Bz2WikiParser.parse(value.toString());
            }
            catch (Exception ee){
                ;
            }
            context.write( new Text(pageName), sol);

            if(sol.toString().length()>2) {
                String strsol = sol.toString().substring(1, sol.toString().length() - 1);
                String[] alladjacenturls = strsol.split(",");
                for (String adjurl : alladjacenturls) {
                    context.write(new Text(adjurl.trim()), new Text(" []"));
                }
            }
        }
    }

    // reducer for parcer
    public static class ParseReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Text answer = new Text();
            StringBuilder s = new StringBuilder();
            for (Text v: values){
                if(v.toString().contains("[]")) continue;
                s.append(v.toString());
            }
            String returning="";

            if(s.length()==0){
                returning = " []";
            }
            else{
                returning= s.toString();
            }
            String newkey = key.toString() + " ||| -1 |||   ";
            if(!key.toString().contains("~")){
           context.write(new Text(newkey),new Text(returning));
           context.getCounter(totalLinks.numberOfLinks).increment(1);
            }


        }
    }


    // page rank mapper
    public static class PageRankMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] splitline = value.toString().trim().split("\\|\\|\\|");
            double n =Double.parseDouble(context.getConfiguration().get("InitialPR"));
            String url = splitline[0].trim();
            String pr = splitline[1].trim();
            Double pageRank = Double.parseDouble(pr);
            if(splitline.length<3){
                System.out.print("error");
            }
            String adj = splitline[2].trim();
            if(pageRank == -1){
                double ad = 1.0/n;
                Text newvalue = new Text(String.valueOf(ad)+"||| "+adj);
                context.write(new Text(url), newvalue);
            }
            else{
                Text newvalue = new Text(pageRank.toString()+"||| "+adj);
                context.write(new Text(url), newvalue);
            }

            String sadj = "";

            // case where link has values in adjacency list
            if(adj.length()>2) {
                sadj = adj.substring(1, adj.length() - 1);
                String[] alladj = sadj.split(",");
                // loop on all links in adj list
                for (String s: alladj){
                    String ss = s.trim();

                    // adj list
                        if(pageRank==-1){
                            Text adjurl = new Text(ss);
                            double adjcontribution = (1.0/n)*(1.0/alladj.length);
                            context.write(adjurl, new Text(String.valueOf(adjcontribution)+"||| []"));
                        }
                        else{
                            Text adjurl = new Text(ss);
                            double delta = Double.parseDouble(context.getConfiguration().get("delta"));
                            double adjcontribution = (pageRank+(0.85* delta / n))/alladj.length;
                            context.write(adjurl, new Text(String.valueOf(adjcontribution)+"||| []"));
                        }



                }
            }

            // case where link has no values in adjacency list
            else{
                    //dangling
                if(pageRank== -1){

                    Double d = 1.0/n;
                    int x=0;
                    context.write(new Text("dummy"), new Text(String.valueOf(d)+"||| []"));
                }
                else {


                    double delta = Double.parseDouble(context.getConfiguration().get("delta"));

                    double del = (0.85 * delta / n) + pageRank;
                    int x=0;
                    context.write(new Text("dummy"), new Text(String.valueOf(del) + "||| []"));
                }
                }

        }
    }

    // reducer for page rank
    public static class PageRankReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            // reducer code
            double sum = 0.0;
            String adj= " []";
            if(key.toString().compareTo("dummy")==0){
                for(Text v: values){
                    String sval = v.toString();
                    String[] val = sval.split("\\|\\|\\|");
                    double pr = Double.parseDouble(val[0]);
                    sum += pr;

                }
                // saving precision
                sum *= 1000000000000000L;
                long l = (long) sum;
                context.getCounter(totalLinks.delta).setValue(l);
            }

            else{
                for(Text v: values){
                    String sval = v.toString();
                    String[] val = sval.split("\\|\\|\\|");
                    if (val.length<2){
                        System.out.print("sfgv");
                    }
                    if (!val[1].contains("[]")){
                        // adjacency
                        adj = val[1].trim();
                    }
                    else{
                        double pr = Double.parseDouble(val[0].trim());
                        sum += pr;
                    }
                }
                double n =  Double.parseDouble(context.getConfiguration().get("InitialPR"));
                double returnVal = (0.15/n) + (0.85*sum);
                if(returnVal>1){
                    System.out.print("Dfv");
                }
                context.write(key, new Text("|||"+String.valueOf(returnVal) + "||| " + adj));
            }

        }
    }

    // top k algo mappre
    public static class TopKMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        // Our output key and value Writables
        private TreeMap<DoubleWritable, Text> repToRecordMap;
        protected void setup(Mapper.Context context) throws IOException, InterruptedException{
            repToRecordMap = new TreeMap<DoubleWritable, Text>();
        }
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Parse the input string into a nice map

            String svalue = value.toString();
            String[] allvalues = svalue.split("\\|\\|\\|");
            double pageRank = Double.parseDouble(allvalues[1].trim());
            Text url = new Text(allvalues[0].trim());
            if(!repToRecordMap.containsValue(url)) {
                repToRecordMap.put(new DoubleWritable(pageRank), url);
            }

            if (repToRecordMap.size() > 100) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (Map.Entry<DoubleWritable, Text> t : repToRecordMap.entrySet()) {
                String k = t.getKey().toString();
                String value = t.getValue().toString();
                String res = k + "|||"+value;
                Text tres = new Text(res);
                context.write(new Text("dummy"), tres);

            }
        }
    }


    // top k algo reducer
    public static class TopKReducer extends
            Reducer<Text, Text, Text, Text> {

        private TreeMap<DoubleWritable, Text> repToRecordMap = new TreeMap<DoubleWritable, Text>();
        private TreeMap<DoubleWritable,Text> reverseMap = new TreeMap<DoubleWritable,Text>(Collections.reverseOrder());

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            for (Text value : values) {

                Text t = new Text(value);
                String[] allVal = value.toString().split("\\|\\|\\|");
                double rank = Double.parseDouble(allVal[0].trim());
                if(!repToRecordMap.containsValue(value)) {
                    repToRecordMap.put(new DoubleWritable(rank), t);
                }
                if (repToRecordMap.size() > 100) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
            reverseMap.putAll(repToRecordMap);
            for (Map.Entry<DoubleWritable, Text> t : reverseMap.entrySet()) {
                context.write(t.getValue(), new Text(""));
            }
        }
    }

}
