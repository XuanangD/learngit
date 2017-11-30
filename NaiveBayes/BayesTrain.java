package v2;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

public class BayesTrain {
	/**
	 * MapReduce计算每一类文档中的单词及其出现次数
	 * @author ding
	 *<<类名:文档名>,word1 word2...>  to  <<类名:word>,Counts>
	 */
	public static class WordsinClassMap extends Mapper<Text, Text, Text, IntWritable>{
		private Text newKey = new Text();
		private final IntWritable one = new IntWritable(1);
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{		
			int index = key.toString().indexOf(":");//训练集key=ClassName:DocID
			String Class = key.toString().substring(0, index);
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				newKey.set(Class + ":" + itr.nextToken());//设置新键值key为<类名:单词>,value计数(统计各个类中各个单词的数量)
				context.write(newKey, one);
			}
		}	
	}
	public static class WordsinClassReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable value:values){
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
			System.out.println(key + "\t" + result);
		}
	}
	
	/**
	 * MapReduce计算每类文档出现的单词总数
	 * @author ding
	 *<<类名:word>,Counts>  to  <类名,Totalcount>
	 */
	public static class TotalWordsinClassMap extends Mapper<Text, IntWritable, Text, IntWritable> {
		private Text newKey = new Text();
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException{
			int index = key.toString().indexOf(":");
			newKey.set(key.toString().substring(0, index));
			context.write(newKey, value);
		}
	}
	public static class TotalWordsinClassReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
	    public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable value : values) {            	
	            sum += value.get();
	        }
	        result.set(sum);            
	        context.write(key, result); 
	        System.out.println(key +"\t"+ result);
	    }
	}
	/**
	 * MapReduce计算训练文档中的出现的单词种类数
	 * @author ding
	 *<<类名:word>,Counts>  to  <word,1>
	 */
	public static class WordsCountsMap extends Mapper<Text, IntWritable, Text, IntWritable> {
	    private Text newKey = new Text();		
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException{
			int index = key.toString().indexOf(":");
			newKey.set(key.toString().substring(index+1, key.toString().length()));//设置新键值key为<word>
			context.write(newKey, value);
		}
	}
	public static class WordsCountsReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable one = new IntWritable(1);
	    public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {	        
	        context.write(key, one); 
	        System.out.println(key +"\t"+ one);
	    }
	}
	
	/**
	 * 计算先验概率
	 * @author ding
	 *
	 */
	public static void GetPriorProbably(String[] otherArgs) throws IOException{
		Configuration conf = new Configuration();			
		String filePath = otherArgs[2]+"/part-r-00000";
		String filePath1 = otherArgs[4];
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		Path path = new Path(filePath);
		Path path1 = new Path(filePath1);
		SequenceFile.Reader reader = null;
		SequenceFile.Writer writer = null;
		double totalWords = 0;
		try{
		
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			IntWritable value = (IntWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			writer = SequenceFile.createWriter(fs, conf, path1, key.getClass(),DoubleWritable.class);
			long position = reader.getPosition();//设置标记点，标记文档起始位置，方便后面再回来遍历
			while(reader.next(key,value)){
				totalWords += value.get();//得到训练集总单词数
			}
			
			reader.seek(position);//重置到前面定位的标记点
			while(reader.next(key,value)){
				writer.append(key, new DoubleWritable(value.get()/totalWords));//P(c)=类c下的单词总数/整个训练样本的单词总数
				//System.out.println(key+":"+"\t"+value.get()/totalWords);
			}
		}finally{
			IOUtils.closeStream(writer);
			IOUtils.closeStream(reader);
		}
		
		
	}
	/**
	 * 计算似然概率
	 * @author ding
	 *
	 */
	public static void GetLikelihoodProbably(String[] otherArgs) throws IOException{
		Configuration conf = new Configuration();		
		HashMap<String, Double> ClassTotalWords = new HashMap<String, Double>();//每个类及类对应的单词总数
		
		String wordcountPath = otherArgs[2]+"/part-r-00000";
		String totalwordPath = otherArgs[3]+"/part-r-00000";
		String wordinclassPath = otherArgs[1]+"/part-r-00000";
		String likelihoodPath = otherArgs[5];
		double TotalDiffWords = 0.0;
		
		FileSystem fs1 = FileSystem.get(URI.create(wordcountPath), conf);
		Path path1 = new Path(wordcountPath);	
		SequenceFile.Reader reader1 = null;
		try{
			reader1 = new SequenceFile.Reader(fs1, path1, conf);
			Text key1 = (Text)ReflectionUtils.newInstance(reader1.getKeyClass(), conf);
			IntWritable value1 = (IntWritable)ReflectionUtils.newInstance(reader1.getValueClass(), conf);
			while(reader1.next(key1,value1)){
				ClassTotalWords.put(key1.toString(), value1.get()*1.0);
				System.out.println(key1.toString() + "\t" + value1.get());
			}
		}finally{
			IOUtils.closeStream(reader1);
		}
		
		FileSystem fs2 = FileSystem.get(URI.create(totalwordPath), conf);
		Path path2 = new Path(totalwordPath);
		SequenceFile.Reader reader2 = null;
		try{
			reader2 = new SequenceFile.Reader(fs2, path2, conf);
			Text key2 = (Text)ReflectionUtils.newInstance(reader2.getKeyClass(), conf);
			IntWritable value2 = (IntWritable)ReflectionUtils.newInstance(reader2.getValueClass(), conf);
			while(reader2.next(key2,value2)){
				TotalDiffWords += value2.get();
			}	
			System.out.println(TotalDiffWords);
		}finally{
			IOUtils.closeStream(reader2);
		}
		
		FileSystem fs3 = FileSystem.get(URI.create(wordinclassPath), conf);
		Path path3 = new Path(wordinclassPath);
		Path path4 = new Path(likelihoodPath);
		SequenceFile.Reader reader3 = null;
		SequenceFile.Writer writer = null;
		try{
			reader3 = new SequenceFile.Reader(fs3, path3, conf);
			Text key3 = (Text)ReflectionUtils.newInstance(reader3.getKeyClass(), conf);
			IntWritable value3 = (IntWritable)ReflectionUtils.newInstance(reader3.getValueClass(), conf);
			Text newKey = new Text();
			writer = SequenceFile.createWriter(fs3, conf, path4, Text.class,DoubleWritable.class);
			while(reader3.next(key3,value3)){
				int index = key3.toString().indexOf(":");
				newKey.set(key3.toString().substring(0, index));//得到单词所在的类
				writer.append(key3, new DoubleWritable((value3.get()+1)/(ClassTotalWords.get(newKey.toString())+TotalDiffWords)));
				                  //<<class:word>,wordcounts/(classTotalNums+TotalDiffWords)>
				//System.out.println(key3.toString() + " \t" + (value3.get()+1) + "/" + (ClassTotalWords.get(newKey.toString())+ "+" +TotalDiffWords));
			}
			//对于同一个类别没有出现过的单词的概率一样，1/(ClassTotalWords.get(class) + TotalDiffWords)
			//遍历类，每个类别中再加一个没有出现单词的概率，其格式为<class,probably>
			for(Map.Entry<String,Double> entry:ClassTotalWords.entrySet()){
				writer.append(new Text(entry.getKey()), new DoubleWritable(1.0/(ClassTotalWords.get(entry.getKey().toString()) + TotalDiffWords)));
				//System.out.println(entry.getKey().toString() +"\t"+ 1.0+"/"+(ClassTotalWords.get(entry.getKey().toString()) +"+"+ TotalDiffWords));
			}
		}finally{
			IOUtils.closeStream(writer);
			IOUtils.closeStream(reader3);
		}

	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 6){
			System.err.println("Usage: Trainset classword wordcount diffword prior likelihood");
			System.exit(6);
		}
		
		FileSystem hdfs = FileSystem.get(conf);

		Path path1 = new Path(otherArgs[1]);
		if(hdfs.exists(path1))
			hdfs.delete(path1, true);
		Job job1 = new Job(conf, "WordsinClass");
		job1.setJarByClass(BayesTrain.class);
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapperClass(WordsinClassMap.class);
		job1.setMapOutputKeyClass(Text.class);//map阶段的输出的key 
		job1.setMapOutputValueClass(IntWritable.class);//map阶段的输出的value 
		job1.setReducerClass(WordsinClassReduce.class);
		job1.setOutputKeyClass(Text.class);//reduce阶段的输出的key 
		job1.setOutputValueClass(IntWritable.class);//reduce阶段的输出的value 
		//加入控制容器 
		ControlledJob ctrljob1 = new  ControlledJob(conf);
		ctrljob1.setJob(job1);
		//job1的输入输出文件路径
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, path1);
		
		Path path2 = new Path(otherArgs[2]);
		if(hdfs.exists(path2))
			hdfs.delete(path2, true);
		Job job2 = new Job(conf, "TotalWordsinClass");
		job2.setJarByClass(BayesTrain.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		job2.setMapperClass(TotalWordsinClassMap.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setReducerClass(TotalWordsinClassReduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		//加入控制容器 
		ControlledJob ctrljob2 = new ControlledJob(conf);
		ctrljob2.setJob(job2);
		//job2的输入输出文件路径
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1] + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, path2);
		
		Path path3 = new Path(otherArgs[3]);
		if(hdfs.exists(path3))
			hdfs.delete(path3, true);
		Job job3 = new Job(conf, "WordsCounts");
		job3.setJarByClass(BayesTrain.class);
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(SequenceFileOutputFormat.class);
		job3.setMapperClass(WordsCountsMap.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);
		job3.setReducerClass(WordsCountsReduce.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		//加入控制容器 
		ControlledJob ctrljob3 = new ControlledJob(conf);
		ctrljob3.setJob(job3);
		//job3的输入输出文件路径
		FileInputFormat.addInputPath(job3, new Path(otherArgs[1] + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job3, path3);
		
		Path path4 = new Path(otherArgs[4]);
		if(hdfs.exists(path4))
			hdfs.delete(path4, true);
		Path path5 = new Path(otherArgs[5]);
			if(hdfs.exists(path5))
				hdfs.delete(path5, true);
		//作业之间依赖关系
		ctrljob2.addDependingJob(ctrljob1);
		ctrljob3.addDependingJob(ctrljob1);
		
		//主的控制容器，控制上面的子作业 		
		JobControl jobCtrl = new JobControl("BayesTrain");
		//添加到总的JobControl里，进行控制
		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);
		jobCtrl.addJob(ctrljob3);
		//在线程启动
	    Thread  theController = new Thread(jobCtrl); 
	    theController.start(); 
	    while(true){
	        if(jobCtrl.allFinished()){//如果作业成功完成，就打印成功作业的信息 
	        	System.out.println(jobCtrl.getSuccessfulJobList()); 
	        	jobCtrl.stop(); 
	        	break; 
	        }
	    } 
		GetPriorProbably(otherArgs);
		GetLikelihoodProbably(otherArgs);
	}
	
}
