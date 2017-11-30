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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
/**
 * 利用训练学习到的先验概率和似然概率对测试文档进行贝叶斯分类
 * @author ding
 *
 */
public class BayesClassify {
	/**
	 * 从文件中读取先验概率和似然概率
	 * @param otherArgs
	 * @throws IOException 
	 */
	private static void GetProbably(String[] otherArgs) throws IOException {
		Configuration conf = new Configuration();			
		String priorPath = otherArgs[0];
		FileSystem fs = FileSystem.get(URI.create(priorPath), conf);
		Path path1 = new Path(priorPath);
		SequenceFile.Reader reader = null;
		//获取先验概率
		try {
			reader = new SequenceFile.Reader(fs, path1, conf);
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			DoubleWritable value = (DoubleWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while(reader.next(key,value)){
				priorProbably.put(key.toString(), value.get());
				System.out.println(key+":"+"\t"+value.get());
			}
		}finally{
			IOUtils.closeStream(reader);
		}
		String likelihoodPath = otherArgs[1];
		FileSystem fs1 = FileSystem.get(URI.create(likelihoodPath), conf);
		Path path2 = new Path(likelihoodPath);
		SequenceFile.Reader reader1 = null;
		//获取似然概率
		try {
			reader1 = new SequenceFile.Reader(fs1, path2, conf);
			Text key1 = (Text)ReflectionUtils.newInstance(reader1.getKeyClass(), conf);
			DoubleWritable value1 = (DoubleWritable)ReflectionUtils.newInstance(reader1.getValueClass(), conf);
			while(reader1.next(key1,value1)){
				likelihoodProbably.put(key1.toString(), value1.get());
				System.out.println(key1+":"+"\t"+value1.get());
			}
		}finally{
			IOUtils.closeStream(reader1);
		}
	}
	
	/**
	 * Map计算文档的各个分类的概率<文档名,<类名:概率>>
	 * @author ding
	 *
	*/
	public static class ClassifyMap extends Mapper<Text, Text, Text, Text> {
			
		private Text newKey = new Text();
		private Text newValue = new Text();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			int index = key.toString().indexOf(":");
			String docID = key.toString().substring(index+1, key.toString().length());
			for(Map.Entry<String, Double> entry:priorProbably.entrySet()){//外层循环遍历所有类别
				String mykey = entry.getKey();//类名
				newKey.set(docID);//新的键值的key为<文档名>
				double tempvalue = Math.log(entry.getValue());//构建临时键值对的value为各概率相乘,转化为各概率取对数再相加
				StringTokenizer itr = new StringTokenizer(value.toString());
				while(itr.hasMoreTokens()){//内层循环遍历一份测试文档中的所有单词	
					String tempkey = mykey + ":" + itr.nextToken();//构建临时键值对<class:word>,在wordsProbably表中查找对应的概率
					if(likelihoodProbably.containsKey(tempkey)){
						//如果测试文档的单词在训练集中出现过，则直接加上之前计算的概率
						tempvalue += Math.log(likelihoodProbably.get(tempkey));
					}
					else{//如果测试文档中出现了新单词则加上之前计算新单词概率
						tempvalue += Math.log(likelihoodProbably.get(mykey));						
					}
				}
				newValue.set(mykey + ":" + tempvalue);//新的键值的value为<类名:概率>
				context.write(newKey, newValue);//一份文档遍历在一个类中遍历完毕,则将结果写入文件
			}
		}
	}
	/**
	 * Reduce比较文档各个分类的概率，决定分类 <文档名,类名>
	 * @author ding
	 *
	 */
	public static class ClassifyReduce extends Reducer<Text, Text, Text, Text> {
		private Text newValue = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			boolean flag = false;//标记,若第一次循环则先赋值,否则比较若概率更大则更新
			String tempClass = null;
			double tempProbably = 0.0;
			for(Text value:values){
				int index = value.toString().indexOf(":");
				if(flag != true){//循环第一次
					tempClass = value.toString().substring(0, index);
					tempProbably = Double.parseDouble(value.toString().substring(index+1, value.toString().length()));
					flag = true;
				}else{//否则当概率更大时就更新tempClass和tempProbably
					if(Double.parseDouble(value.toString().substring(index+1, value.toString().length())) > tempProbably){
						tempClass = value.toString().substring(0, index);
						tempProbably = Double.parseDouble(value.toString().substring(index+1, value.toString().length()));
					}
				}
			}							
			newValue.set(tempClass);
			context.write(key, newValue);
			System.out.println(key + "\t" + newValue);
		}
	}
	
	static String[] otherArgs; 
	private static HashMap<String,Double> priorProbably = new HashMap<String,Double>();
	private static HashMap<String, Double> likelihoodProbably = new HashMap<String, Double>();
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 4){
			System.err.println("Usage: prior likelihood test result");
			System.exit(4);
		}
		GetProbably(otherArgs);
		FileSystem hdfs = FileSystem.get(conf);
		Path path1 = new Path(otherArgs[3]);
		if(hdfs.exists(path1))
			hdfs.delete(path1, true);
		Job job1 = new Job(conf, "Classify");
		job1.setJarByClass(BayesClassify.class);
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapperClass(ClassifyMap.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setReducerClass(ClassifyReduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		//加入控制容器 
		ControlledJob ctrljob1 = new ControlledJob(conf);
		ctrljob1.setJob(job1);
		//job1的输入输出文件路径
		FileInputFormat.addInputPath(job1, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job1, path1);
		
		JobControl jobCtrl = new JobControl("BayesClassify");
		jobCtrl.addJob(ctrljob1);
		//线程启动
	    Thread  theController = new Thread(jobCtrl); 
	    theController.start(); 
	    while(true){
	        if(jobCtrl.allFinished()){//如果作业成功完成，就打印成功作业的信息 
	        	System.out.println(jobCtrl.getSuccessfulJobList()); 
	        	jobCtrl.stop(); 
	        	break; 
	        }
	    }  
	}
	
}
