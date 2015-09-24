/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dlyle.testh20;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import water.H2O;
import water.H2OApp;

public class H2OTest extends Configured implements Tool{

  public static class H2OMapper
       extends Mapper<Object, Text, Text, Text>{
    
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        String runid = "H2o_"+context.getUser()+'_'+System.currentTimeMillis();
        
        if(!"false".equals(context.getConfiguration().get("h2o.start", "false"))){
            String flowDir = context.getConfiguration().get("mapreduce.cluster.local.dir");
            context.getCounter("Test: ", flowDir).setValue(1);
            H2OApp.main(new String[]{"-name", runid, "-flow_dir", flowDir});   
            if(!"false".equals(context.getConfiguration().get("h2o.stop","false"))){
                context.getCounter("Test: " ,H2O.getIpPortString()).setValue(1);        
                H2O.shutdown(0);
            }
        }    
        
    }
                
  }

 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new H2OTest(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "H2O Test");
        job.setJarByClass(H2OTest.class);
        job.setMapperClass(H2OMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
  
}