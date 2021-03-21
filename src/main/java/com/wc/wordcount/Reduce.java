/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.wc.wordcount;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Work
 */
public class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    
    @Override
    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
        
        int sum = 0;
        
        Iterator<IntWritable> iter = values.iterator();
        
        while(iter.hasNext()){
            sum += iter.next().get();
        }
        
        context.write(key,new IntWritable(sum));
    }
}
