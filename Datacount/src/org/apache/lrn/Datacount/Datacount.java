package org.apache.lrn.Datacount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Datacount {//继承泛型类Mapper

	private static class DataMapper extends Mapper<Object, Text, Text, Text>{	
		//public Text word=new Text();
		//实现map函数   
		public void map (Object key, Text value, Context context
                 ) throws IOException, InterruptedException {	
			//java分隔符获得字符串
			//StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			String line=value.toString();  	//将输入文本转化为字符串类型
			String[] itr=line.split("\n");	//按照空格符分割字符串
			for(int i=0;i<itr.length;i++){  
				String[] str=itr[i].split("\t");  // 对每行字符串进行分割，分隔符是制表符“\t”
				
				if(str.length!=7){
					System.out.println("Error: the unreasonable data!");
					return ;
				}
				
				String userid=str[1];  //user
				String spname=str[4];  //server
				String upload=str[5];  //upload
				String download=str[6];//download
				String one="1"; //counts
				
				Text kw=new Text(userid+"\t"+spname);    //自定义Text数据类型key
				Text val=new Text(one+"\t"+upload+"\t"+download); //自定义Text数据类型value
				
				context.write(kw,val);	
			}
		}
	}

	//继承泛型类Reducer
	public static class DataReducer extends Reducer<Text,Text,Text,Text> {

		//实现reduce
		public void reduce(Text key, Iterable<Text> values, 
                    Context context
                    ) throws IOException, InterruptedException {
			long up=0;
			long down=0;
			int sum=0;
			//循环values
			for (Text array : values) {
				String[] ary=array.toString().split("\t");
				sum+=Integer.parseInt(ary[0]);
				up+= Long.parseLong(ary[1]) ;
				down+= Long.parseLong(ary[2]) ;
			}
			//System.out.println(sum);
			//System.out.println(up);
			//System.out.println(down);
			//System.out.println(key);
			
			context.write(key,new Text(sum+"\t"+up+"\t"+down));			
		}
	}
	
	public static void main(String[] args) throws Exception{

		//实例化Configuration
		Configuration conf = new Configuration();
		//总结上面：返回数组【一组路径】
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//如果只有一个路径，则输出需要有输入路径和输出路径
		if (otherArgs.length != 2) {
			System.err.println("Usage: datacount <in> <out>");
			System.exit(2);
		}
		
		//实例化job

		Job job = new Job(conf, "data count");
		//为了能够找到WordCount这个类
		job.setJarByClass(Datacount.class);
		//指定map类型
		job.setMapperClass(DataMapper.class);
		
		job.setCombinerClass(DataReducer.class);
		
		job.setReducerClass(DataReducer.class);

		//reduce输出Key的类型，是Text
		job.setOutputKeyClass(Text.class);
		// reduce输出Value的类型
		job.setOutputValueClass(Text.class);	
		//添加输入路径	
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));	
		//添加输出路径
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));		
		
		//提交job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}



	