package com.wordcount;


	import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.StringTokenizer;

	import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

	public class Assignment1 {

	  public static class TokenizerMapper
	       extends Mapper<Object, Text, Text, Text>{
        private static String[] data; 
        private static String dest; 
	    private final static Text one = new Text("1");
	  
@Override
protected void setup(Mapper<Object, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	super.setup(context);
	dest=context.getConfiguration().get("Dest");
	data=context.getConfiguration().getStrings("source");
}
	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	
	    
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	    	  String s= itr.nextToken();
	    	  String k= itr.nextToken();
	    	  for(String a: data)
	    	  { 
	    		  if(s.compareTo(a)==0) { 	    		
	    			  context.write( one,new Text(k));
	    		  }	 
	    	  }
	      
	      }
	  
	    }
	  }

	  public static class IntSumReducer
	       extends Reducer<Text,Text,Text,Text> {
	
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	
	}

	    public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      
	   // ArrayList<String> as = new ArrayList<String>();
	    String s="";
	      for (Text val : values) {
	    	s=s+" "+val.toString();
	    
	      }
			
	      context.write(new Text("2"),new Text(s) );
	      System.out.print( "\n"+ key+" workigoing "+s+"\n");
	    
	    }
	  }

	  
	  
	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    /////////////////////////
	    ArrayList<String> quer = new ArrayList<String>(); 
	    ArrayList<String> result = new ArrayList<String>();
	    Path Dtest = new Path("/mapreduce/assginment1/DTest");
	    Path output = new Path("\\output\\assignment1\\");
	    
	    FileSystem hdfs = Dtest.getFileSystem(conf);
		FSDataInputStream inputpathstreamsecmatrix = hdfs.open(Dtest);
		InputStreamReader isra = new InputStreamReader(inputpathstreamsecmatrix, "ISO-8859-1");	
		BufferedReader dbr = new BufferedReader(isra);
		Scanner scanner1 = new Scanner(dbr);
		while(scanner1.hasNext())
		{
			quer.add(scanner1.nextLine());
		}
		
	
		scanner1.close();
		dbr.close();
		isra.close();
		inputpathstreamsecmatrix.close();
		System.gc();
		///////////
		
		
		  if (hdfs.exists(output))
				hdfs.delete(output, true);
	   int k=0;
	    for(String singlequery : quer) {
	    	if (hdfs.exists(output))
				hdfs.delete(output, true);
		    
	    	if(k==2)break;
	    	k++;
	    String[] data=singlequery.split(" ");
	    String[] arr = new String[1];
	    arr[0]=data[0];
	    conf.unset("source");	 
	    conf.setStrings("source", arr);
	    conf.unset("Dest");	 
	    String destination = data[1];
	    conf.set("Dest",data[1]);
	    conf.setBoolean("searchstart", true);
	    boolean searchstart = true;
	    int i=0;
	    Path outputarr = new Path("\\output\\assignment1\\part-r-00000");
	    hdfs = outputarr.getFileSystem(conf);
	    
	    ArrayList<String> recur = new ArrayList<String>();
	    recur.clear();
	    while(searchstart) {
	    ///////////////////////////
	    
	    	if(i>0)
	    	{
	    		
	    			FSDataInputStream outputarrs =hdfs.open(outputarr);
	    			InputStreamReader israc = new InputStreamReader(outputarrs, "ISO-8859-1");	
	    			 dbr = new BufferedReader(israc);
	    			 scanner1 = new Scanner(dbr);
	    			// if(scanner1.hasNext())
	    			// {
	    				 String s = scanner1.nextLine();
	    				 StringTokenizer st = new StringTokenizer(s);
	    				 String[] cd = new String[st.countTokens()];
	    				 st.nextToken();
	    				 int si=0;
	    				 while(st.hasMoreTokens())
	    				 {
	    					String ka=st.nextToken().toString();
	    					 if(!recur.contains(ka)) {
	    					 cd[si]=ka;
	    					 recur.add(ka);
	    					 }
	    					 if(0==destination.compareTo(ka))
	    					 {
	    						 searchstart=false;
	    						 break;
	    					 }
	    					  System.out.print( "\n"+ si+"  "+cd[si] + "\n");
	    					 si++;
	    					
	    				 }
	    				 
	    			// }
	    			 conf.unset("source");	 
    				 conf.setStrings("source", cd); 
	    			 scanner1.close();
	    			 
	    			// System.out.print( "\n"+ "?????ccc?????" + "\n");
	    			 outputarrs.close();
	    			 israc.close();
	    			 dbr.close();
	    			 
	    			 System.gc();
	    		
	    	}
	    	
	    	
	    	if (hdfs.exists(output))
				hdfs.delete(output, true);
	    	if(!searchstart) {
		    	
		    	result.add(i+"");
		    	break;
		    }   
	    Job job = Job.getInstance(conf, "Ass1"+i);
	    job.setJarByClass(Assignment1.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, output);
	    job.waitForCompletion(true) ;
	    i++;
	  //  searchstart=conf.getBoolean("searchstart",true);
	    System.out.print( "\n"+ i+" workigoing "+k + "\n");
	    if(i==10) {result.add("-1");break;
	    }
	    if(!searchstart) {
	    	
	    	result.add(i+"");
	    	break;
	    }
	    }
	    if(k>10)break;
	    }
	  
	  
	  for(String s: result)
		  System.out.print(s+" ");
	  System.exit(1);
	  }
	}

