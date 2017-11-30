package v2;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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



public class Evaluation {

	/**
	 * MapReduce获得测试文档中的分类情况
	 * @author ding
	 *<<类名:文档名>,word1 word2...> to <类名,文档名>
	 */
	public static class OriginalMap extends Mapper<Text, Text, Text, Text> {
		private Text newKey = new Text();
		private Text newValue = new Text();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{			
			int index = key.toString().indexOf(":");
			newKey.set(key.toString().substring(0, index));
			newValue.set(key.toString().substring(index+1, key.toString().length()));
			context.write(newKey, newValue);
			System.out.println(newKey + "\t" + newValue);
		}
	}
	
	/**
	 * MapReduce获得文档分类后的分类情况
	 * @author ding
	 *
	 */
	public static class ClassifiedMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{		
			context.write(value, key);
			System.out.println(value + "\t" + key);
		}
	
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();		
		public void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException{
			//生成文档列表
			String fileList = new String();
			for(Text value:values){
				fileList += value.toString() + ";";
			}
			result.set(fileList);
			context.write(key, result);
			System.out.println(key + "\t" + result);
		}
	}
	

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: test original result classfied");
			System.exit(4);
		}		
		FileSystem hdfs = FileSystem.get(conf);
		
		Path path1 = new Path(otherArgs[1]);
		if(hdfs.exists(path1))
			hdfs.delete(path1, true);
		Job job1 = new Job(conf, "Original");
		job1.setJarByClass(Evaluation.class);
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapperClass(OriginalMap.class);
		job1.setReducerClass(Reduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		//加入控制容器 
		ControlledJob ctrljob1 = new  ControlledJob(conf);
		ctrljob1.setJob(job1);
		//job1的输入输出文件路径
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		
		Path path2 = new Path(otherArgs[3]);
		if(hdfs.exists(path2))
			hdfs.delete(path2, true);
		Job job2 = new Job(conf, "Classified");
		job2.setJarByClass(Evaluation.class);	
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		job2.setMapperClass(ClassifiedMap.class);
		job2.setReducerClass(Reduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//加入控制容器 
		ControlledJob ctrljob2 = new  ControlledJob(conf);
		ctrljob2.setJob(job2);
		//job2的输入输出文件路径
		FileInputFormat.addInputPath(job2, new Path(otherArgs[2]+"/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
		
		ctrljob2.addDependingJob(ctrljob1);
		
		JobControl jobCtrl = new JobControl("NaiveBayes");
		//添加到总的JobControl里，进行控制
		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);
		
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
	    Evaluations(conf, otherArgs[1]+"/part-r-00000", otherArgs[3]+"/part-r-00000");
	   
	}

	public static void Evaluations(Configuration conf, String OriginalPath, String ClassifiedPath) throws IOException{
		FileSystem fs1 = FileSystem.get(URI.create(ClassifiedPath), conf);
		Path path1 = new Path(ClassifiedPath);
		SequenceFile.Reader reader1 = null;
		
		FileSystem fs2 = FileSystem.get(URI.create(OriginalPath), conf);
		Path path2 = new Path(OriginalPath);
		SequenceFile.Reader reader2 = null;
		try{
			reader1 = new SequenceFile.Reader(fs1, path1, conf);//创建Reader对象			
			Text key1 = (Text)ReflectionUtils.newInstance(reader1.getKeyClass(), conf);
			Text value1 = (Text)ReflectionUtils.newInstance(reader1.getValueClass(), conf);
			
			reader2 = new SequenceFile.Reader(fs2, path2, conf);
			Text key2 = (Text)ReflectionUtils.newInstance(reader2.getKeyClass(), conf);
			Text value2 = (Text)ReflectionUtils.newInstance(reader2.getValueClass(), conf);
			
		
			ArrayList<String> ClassNames = new ArrayList<String>();     //依次得到分类的类名
			ArrayList<Integer> TruePositive = new ArrayList<Integer>(); //记录真实情况和经分类后，正确分类的文档数目
			ArrayList<Integer> FalseNegative = new ArrayList<Integer>();//记录属于该类但是没有分到该类的数目
			ArrayList<Integer> FalsePositive = new ArrayList<Integer>();//记录不属于该类但是被分到该类的数目
			
		
			while((reader1.next(key1, value1))&&(reader2.next(key2, value2))){	
				
				ClassNames.add(key1.toString());
				
				String[] values1 = value1.toString().split(";");
				String[] values2 = value2.toString().split(";");									
				
				int TP = 0;
				for(String str1:values1){
					for(String str2:values2){
						if(str1.equals(str2)){
							TP++;
						}
					}
				}
				
				TruePositive.add(TP);
				FalsePositive.add(values1.length - TP);
				FalseNegative.add(values2.length - TP);	
				
				double pp = TP*1.0/values1.length;
				double rr = TP*1.0/values2.length;
				double ff = 2*pp*rr/(pp+rr);
				
				System.out.println(key1.toString() + ":" + key2.toString() + "\t" + values1.length + "\t" + values2.length + 
						"\t" + TP + "\t" + (values1.length-TP) + "\t" + (values2.length-TP) + "\tp=" + pp + ";\tr=" + rr + ";\tf1=" + ff);
						
			}
			
			//Caculate MacroAverage
			double Pprecision = 0.0;
			double Rprecision = 0.0;
			double F1precision = 0.0;

			//Calculate MicroAverage
			int TotalTP = 0;
			int TotalFN = 0;
			int TotalFP = 0;			
			
			System.out.println(ClassNames.size());
			
			for(int i=0; i<ClassNames.size(); i++){			
				//MacroAverage				
				double p1 = TruePositive.get(i)*1.0/(TruePositive.get(i) + FalsePositive.get(i));
				double r1 = TruePositive.get(i)*1.0/(TruePositive.get(i) + FalseNegative.get(i));
				double f1 = 2.0*p1*r1/(p1+r1);
				Pprecision += p1;
				Rprecision += r1;
				F1precision += f1;
								
				//MicroAverage
				TotalTP += TruePositive.get(i);
				TotalFN += FalseNegative.get(i);
				TotalFP += FalsePositive.get(i);
			}
			System.out.println("MacroAverage precision : P=" + Pprecision/ClassNames.size() +";\tR="+ Rprecision/ClassNames.size() +";\tF1="+F1precision/ClassNames.size());
			
			double p2 = TotalTP*1.0/(TotalTP + TotalFP);
			double r2 = TotalTP*1.0/(TotalTP + TotalFN);
			double f2 = 2.0*p2*r2/(p2+r2);
			
			System.out.println("MicroAverage precision : P= " + p2 + ";\tR=" + r2 + ";\tF1=" + f2);
			
		}finally{
			reader1.close();
			reader2.close();
		}		
	}

}
