/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.wc.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Work
 */
public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    @Override
    public void map(LongWritable Key,Text value,Context context) throws IOException, InterruptedException{
        
        String line = value.toString();
        
        StringTokenizer tokens = new StringTokenizer(line);
        
        while(tokens.hasMoreTokens()){
            
            word.set(tokens.nextToken());
            context.write(word,one);
        }
    }
}
