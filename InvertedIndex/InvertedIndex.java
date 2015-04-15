package iip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.InputSplit; 


public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{

    	private Text keyInfo = new Text();
        private Text valueInfo = new Text();
        private FileSplit split;
        private String pattern = "[\\w+]";
        
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
            //获得<key,value>对所属的对象
            split = (FileSplit)context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll(pattern, " "));
            while (itr.hasMoreTokens()) {
                //key值有单词和url组成，如"mapreduce:1.txt"
                keyInfo.set(itr.nextToken()+ ":" +split.getPath().getName().replace(".txt.segmented", ""));
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }
            
        }
    }
    
      
    
    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>{
        private Text info = new Text();
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException {
            //统计词频
            int sum = 0;
            for (Text value:values) {
                sum += Integer.parseInt(value.toString());
            }            
            int splitIndex = key.toString().indexOf(":");
            //重新设置value值由url和词频组成
            info.set(key.toString().substring(splitIndex+1)+":"+sum);
            //重新设置key值为单词
            key.set(key.toString().substring(0,splitIndex));
            context.write(key, info);
        }
    	
    }
    
    
    public static class InvertedIndexReduce extends Reducer<Text, Text, Text, Text> {
            private Text result = new Text();
            public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
                //生成文档列表
                String fileList = new String();
                double sum = 0,count = 0;
                for (Text value:values) {
                	count ++;
                	sum += Integer.parseInt(value.toString().split(":")[1]);
                    fileList += value.toString()+";";
                }
                fileList = "\t"+ sum/count +"," + fileList;
                result.set(fileList);
                context.write(key, result);
            }
            
            
    }
    
    
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "InvertedIndex");
        
        job.setJarByClass(InvertedIndex.class);
                        
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(InvertedIndexMapper.class);
        
        job.setCombinerClass(InvertedIndexCombiner.class);        
        job.setReducerClass(InvertedIndexReduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path("hdfs://114.212.84.141:9000/input/exp2"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://114.212.84.141:9000/outputExp22"));
        
        System.exit(job.waitForCompletion(true)?0:1);

    }

}
