package iip;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TermFrequencySort {

    public static class InvertedIndexMapper extends Mapper<Object, Text, DoubleWritable, Text>{

        private Text valueInfo = new Text();
        
        private static DoubleWritable data = new DoubleWritable();
        
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
            
        	//获得<key,value>对所属的对象
        	int index = value.toString().indexOf(",");
        	String  str = value.toString().substring(0,index);

			String mapkey = str.split("\\s+")[1];
			String mapvalue = str.split("\\s+")[0]+"#"+value.toString().substring(index+1);
			
			data.set(Double.parseDouble(mapkey));
			valueInfo.set(mapvalue);
			
			context.write(data, valueInfo);           
        }
        
    }
    
      
    
    public static class MyPartitioner extends Partitioner<DoubleWritable, Text>{

		@Override
		public int getPartition(DoubleWritable key, Text value, int numPartitions) {
			
			int maxNumber = 7562;
			
			int bound = maxNumber / numPartitions + 1 ;
						
			Double keyNumber = key.get();
									
			for (int i = 0; i < numPartitions; i++) {
							
				if (keyNumber >= bound * (numPartitions-1) ) {
					return (numPartitions-1);
				}
				
				if (keyNumber < bound * (i+1) && keyNumber >= bound * i) {
					return i;
				}				
			}		
			return -1;
		}
    	
    }
    
    
    public static class InvertedIndexReduce extends Reducer<DoubleWritable, Text, Text, Text> {
          
    	private Text keyInfo = new Text();
        private Text valueInfo = new Text();
            
        public void reduce(DoubleWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
                //生成文档列表           	
            for (Text value:values) {
                int index = value.toString().indexOf("#");
                	
                String  reduceKey = value.toString().substring(0,index);
                String  reduceValue = "\t"+ key.toString()+","+ value.toString().substring(index+1);
                	
                keyInfo.set(reduceKey);
                valueInfo.set(reduceValue);
                context.write(keyInfo, valueInfo);
            }               
       }              
    }
    
    
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "InvertedIndexSort");
        
        job.setJarByClass(InvertedIndex.class);
                        
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
              
        job.setMapperClass(InvertedIndexMapper.class);        
        job.setPartitionerClass(MyPartitioner.class);        
        job.setReducerClass(InvertedIndexReduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path("hdfs://114.212.84.141:9000/outputExp22"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://114.212.84.141:9000/outputSort"));
        
        System.exit(job.waitForCompletion(true)?0:1);

    }

}
