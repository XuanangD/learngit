package v2;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
/**
 * 将文件序列化成如下格式
 * JAP:480336newsML.txt	key june jgb futures ended slightly higher comments chairman japan association corporate
 * @author ding
 * 
 */
public class FileSequence {
	/**
	 * 把单个文件序列化，转化为单行字符串的格式
	 * @param file 文件名
	 * @return result 文件内容合并成的字符串
	 * @throws IOException
	 */
	private static String fileToString(File file) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(file));		
		
		String line = null;
		String result = "";
		while((line = reader.readLine()) != null){
			if(line.matches("[a-zA-Z]+")){//过滤掉以数字开头的词
				result += line + " ";//单词之间以空格符隔开
				//System.out.println(line);
			}
		}
		reader.close();
		return result;
	}	
	/**
	 * 将一个文件夹下的所有文件序列化
	 * @param args[0] 输入文件的路径
	 * @param args[1] 输出文件的路径
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		File[] dirs = new File(args[0]).listFiles();
		
		String uri = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		
		Text key = new Text();
		Text value = new Text();
		
		SequenceFile.Writer writer = null;
		try{
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
		
			for(File dir:dirs){
				File[] files = dir.listFiles();
				for(File file:files){
					//key：目录名+":"+文件名					
					key.set(dir.getName() + ":" + file.getName());
					//value：文件内容
					value.set(fileToString(file));
					writer.append(key, value);
					System.out.println(key + "\t" + value);
				}
			}
		}finally{
			IOUtils.closeStream(writer);
		}		
	}

}
