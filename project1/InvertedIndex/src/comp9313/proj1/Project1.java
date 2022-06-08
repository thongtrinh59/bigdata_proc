package comp9313.proj1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import comp9313.proj1.StringPair;
import comp9313.proj1.StringInt;
import comp9313.proj1.Project1.TFIDFCombiner;
import comp9313.proj1.Project1;
import comp9313.proj1.Project1.TFIDFGroupingComparator;
import comp9313.proj1.Project1.TFIDFMapper;
import comp9313.proj1.Project1.TFIDFPartitioner;
import comp9313.proj1.Project1.TFIDFReducer;

public class Project1 {
	public static class TFIDFMapper extends Mapper<Object, Text, StringPair, StringInt> { 
        //composite key StringPair(String, String)
		//first parameter is the term
		//second parameter is the DocID
        private StringPair keyPair = new StringPair();
        //value StringInt(Text, IntWritable)
        //first parameter is the DocID
        //second parameter is number of occurence of a term in the Doc
        private StringInt valuePair = new StringInt();
         
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");             
            ArrayList<String> termArray = new ArrayList<String>();
            while (itr.hasMoreTokens()) {
                termArray.add(itr.nextToken().toLowerCase());
            }
            //skipping empty line in Doc if exist
            if(termArray.size() != 0) {
                int id = 0;
                String docID = new String(termArray.get(id));
                termArray.remove(id);
                for (int i = 0; i < termArray.size(); i++) {
                	keyPair.set(termArray.get(i).toLowerCase(), docID );
                	valuePair.set(new Text(docID), new IntWritable(1));
                	context.write(keyPair, valuePair);
                }
            }
        }       
    }
//	combiner
	public static class TFIDFCombiner extends Reducer<StringPair, StringInt, StringPair, StringInt> {
                 
        public void reduce(StringPair key, Iterable<StringInt> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (StringInt val : values) {
            	int temp = val.getSecond().get();
                sum += temp;
            }

            StringInt combineValue = new StringInt(new Text(key.getSecond()), new IntWritable(sum));

            context.write(key, combineValue);
        }       
    }
	
//	patitioner
    public static class TFIDFPartitioner extends Partitioner<StringPair, StringInt>{
        @Override
        public int getPartition(StringPair key, StringInt value, int numPartitions) { 
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
//    group comparator, group natural key in composite key
    public static class TFIDFGroupingComparator extends WritableComparator{
    	
    	protected TFIDFGroupingComparator(){
    		super(StringPair.class, true);
    	}
    	//group key by first term of composite key
    	@Override
    	public int compare(WritableComparable str1, WritableComparable str2){
    		
    		StringPair strPair1 = (StringPair) str1;
    		StringPair strPair2 = (StringPair) str2;    	
    		
    		return strPair1.getFirst().compareTo(strPair2.getFirst());
    	}
    }
 
//    reducer
    public static class TFIDFReducer extends Reducer<StringPair, StringInt, Text, Text> {
    	 
  
         public void reduce(StringPair key, Iterable<StringInt> values, Context context)
                 throws IOException, InterruptedException {
        	 
        	 HashMap<String, Integer> map = new HashMap<String, Integer>();
        	     		
        	 Configuration conf = context.getConfiguration();
        	 String param = conf.get("numDoc");
        	 double totalNumberOfDocuments = Double.valueOf(param);
        	 
        	 for(StringInt itr : values) {
        		 String fname = itr.getFirst().toString();
        		 Integer valFreq = itr.getSecond().get();
        		 if (!map.containsKey(fname)) {
        			 map.put(fname, valFreq);
        		 }

        	 }

//        	 sorting to make DocID ascending
        	 Object[] sortkey = map.keySet().toArray();
 			 Arrays.sort(sortkey);
 			 
 			for (int i = 0; i < sortkey.length; i++) {
 				double tf = Double.valueOf(map.get(sortkey[i]));
 				//number of term i in doc j equal length of map
 				double idf = Math.log10((double) totalNumberOfDocuments / (double) sortkey.length);
 				double tf_idf = tf*idf;
 				String tempResult = sortkey[i] + ", " + tf_idf;
 				context.write(new Text(key.getFirst()), new Text(tempResult));
 				
			}
        	 
         }
    }
     
     
    public static void main(String[] args) throws Exception {       
         
        Configuration conf = new Configuration();
        conf.set("numDoc", args[2]);
        Job job = Job.getInstance(conf, "Project 1");
        job.setJarByClass(Project1.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setCombinerClass(TFIDFCombiner.class);
        job.setReducerClass(TFIDFReducer.class);
        //either add this partitioner, or override the hashCode() function in StringPair
        job.setPartitionerClass(TFIDFPartitioner.class);
        job.setMapOutputKeyClass(StringPair.class);
        job.setMapOutputValueClass(StringInt.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setGroupingComparatorClass(TFIDFGroupingComparator.class);
        
        job.setNumReduceTasks(Integer.parseInt(args[3])); 
//        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));     
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        
    }
}
