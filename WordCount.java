import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordCount {

  public static class TokenizerMapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word1 = new Text();
    private Text word2 = new Text();
    private Text word3 = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
FileSplit fileSplit = (FileSplit)context.getInputSplit();
String filename = fileSplit.getPath().getName();

      /*StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }*/
      //initially if the genre is %Romance% and %comedy% - 5th string and  year is between 2001-2005 - 4th str filter
      //if the condition is true the write movie, one
      String tuple = value.toString();
      String[] tuple_array = tuple.split(";");
     // String match genre = "%Comedy%Romance%";
      String genre = tuple_array[4];
      String g[] = genre.split(",");
      int count1 =0;
      int count2 =0;
      int count3 =0;
      int lb1 = Integer.parseInt("2001");
      int ub1 = Integer.parseInt("2005");
      int lb2 = Integer.parseInt("2006");
      int ub2 = Integer.parseInt("2010");
      int lb3 = Integer.parseInt("2011");
      int ub3 = Integer.parseInt("2015");
      String test = tuple_array[3];
      String m = tuple_array[1];
      if(tuple_array.length == 5 && !test.equals("\\N"))
      {
      	     int test_year = Integer.parseInt(test);
	     if(test_year >=lb1 && test_year<=ub1 && m.equals("movie"))
	     {
	      for(String g1 : g)
	      {
		      if(!g1.equals("\\N"))
		      {
		      	  if(g1.equals("Romance"))
		      	  {
		      	  	count1++;
		      	  }
		      	  if(g1.equals("Comedy"))
		      	  {
		      	  	count1++;
		      	  }
		      }
		      if(count1==2)
		      {
		      	  //context.write(new Text(tuple_array[0]),one); 
		      	  System.out.println(tuple_array[0] + " ; " +tuple_array[2] + " ; " +tuple_array[4]);
		      	  String intermidiateKey = "[2001-2005]:Comedy,Romance";
			  word1.set(intermidiateKey);
			  context.write(word1, one);
			  count1 =0;
		      }
	      }
	    }      
      }
      if(tuple_array.length == 5 && !test.equals("\\N"))
      {
      	     int test_year = Integer.parseInt(test);
	     if(test_year >=lb2 && test_year<=ub2 && m.equals("movie"))
	     {
	      for(String g1 : g)
	      {
		      if(!g1.equals("\\N"))
		      {
		      	  if(g1.equals("Romance"))
		      	  {
		      	  	count2++;
		      	  }
		      	  if(g1.equals("Comedy"))
		      	  {
		      	  	count2++;
		      	  }
		      }
		      if(count2==2)
		      {
		      	  //context.write(new Text(tuple_array[0]),one); 
		      	  System.out.println(tuple_array[0] + " ; " +tuple_array[2] + " ; " +tuple_array[4]);
		      	  String intermidiateKey = "[2006-2010]:Comedy,Romance";
			  word2.set(intermidiateKey);
			  context.write(word2, one);
			  count2 =0;
		      }
	      }
	    }      
      }
      if(tuple_array.length == 5 && !test.equals("\\N"))
      {
      	     int test_year = Integer.parseInt(test);
	     if(test_year >=lb3 && test_year<=ub3 && m.equals("movie"))
	     {
	      for(String g1 : g)
	      {
		      if(!g1.equals("\\N"))
		      {
		      	  if(g1.equals("Romance"))
		      	  {
		      	  	count3++;
		      	  }
		      	  if(g1.equals("Comedy"))
		      	  {
		      	  	count3++;
		      	  }
		      }
		      if(count3==2)
		      {
		      	  //context.write(new Text(tuple_array[0]),one); 
		      	  System.out.println(tuple_array[0] + " ; " +tuple_array[2] + " ; " +tuple_array[4]);
		      	  String intermidiateKey = "[2011-2015]:Comedy,Romance";
			  word3.set(intermidiateKey);
			  context.write(word3, one);
			  count3 =0;
		      }
	      }
	    }      
      }
      /*if(genre.contains("Romance"))
      {
    	  //String str = tuple_array[0] + "--" +tuple_array[1] + "--" + tuple_array[2] + "--" + tuple_array[3] + "--" +tuple_array[4];
    	  context.write(new Text(tuple_array[0]),one); 
      }*/
    }
  }
public static class TokenizerMapper2
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word1 = new Text();
    private Text word2 = new Text();
    private Text word3 = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
FileSplit fileSplit = (FileSplit)context.getInputSplit();
String filename = fileSplit.getPath().getName();

      /*StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }*/
      //initially if the genre is %Romance% and %comedy% - 5th string and  year is between 2001-2005 - 4th str filter
      //if the condition is true the write movie, one
      String tuple = value.toString();
      String[] tuple_array = tuple.split(";");
     // String match genre = "%Comedy%Romance%";
      String genre = tuple_array[4];
      String g[] = genre.split(",");
      int count1 =0;
      int count2 =0;
      int count3 =0;
      int lb1 = Integer.parseInt("2001");
      int ub1 = Integer.parseInt("2005");
      int lb2 = Integer.parseInt("2006");
      int ub2 = Integer.parseInt("2010");
      int lb3 = Integer.parseInt("2011");
      int ub3 = Integer.parseInt("2015");
      String test = tuple_array[3];
      String m = tuple_array[1];
      if(tuple_array.length == 5 && !test.equals("\\N"))
      {
      	     int test_year = Integer.parseInt(test);
	     if(test_year >=lb1 && test_year<=ub1 && m.equals("movie"))
	     {
	      for(String g1 : g)
	      {
		      if(!g1.equals("\\N"))
		      {
		      	  if(g1.equals("Action"))
		      	  {
		      	  	count1++;
		      	  }
		      	  if(g1.equals("Thriller"))
		      	  {
		      	  	count1++;
		      	  }
		      }
		      if(count1==2)
		      {
		      	  //context.write(new Text(tuple_array[0]),one); 
		      	  System.out.println(tuple_array[0] + " ; " +tuple_array[2] + " ; " +tuple_array[4]);
		      	  String intermidiateKey = "[2001-2005]:Action,Thriller";
			  word1.set(intermidiateKey);
			  context.write(word1, one);
			  count1 =0;
		      }
	      }
	    }      
      }
      if(tuple_array.length == 5 && !test.equals("\\N"))
      {
      	     int test_year = Integer.parseInt(test);
	     if(test_year >=lb2 && test_year<=ub2 && m.equals("movie"))
	     {
	      for(String g1 : g)
	      {
		      if(!g1.equals("\\N"))
		      {
		      	  if(g1.equals("Action"))
		      	  {
		      	  	count2++;
		      	  }
		      	  if(g1.equals("Thriller"))
		      	  {
		      	  	count2++;
		      	  }
		      }
		      if(count2==2)
		      {
		      	  //context.write(new Text(tuple_array[0]),one); 
		      	  System.out.println(tuple_array[0] + " ; " +tuple_array[2] + " ; " +tuple_array[4]);
		      	  String intermidiateKey = "[2006-2010]:Action,Thriller";
			  word2.set(intermidiateKey);
			  context.write(word2, one);
			  count2 =0;
		      }
	      }
	    }      
      }
      if(tuple_array.length == 5 && !test.equals("\\N"))
      {
      	     int test_year = Integer.parseInt(test);
	     if(test_year >=lb3 && test_year<=ub3 && m.equals("movie"))
	     {
	      for(String g1 : g)
	      {
		      if(!g1.equals("\\N"))
		      {
		      	  if(g1.equals("Action"))
		      	  {
		      	  	count3++;
		      	  }
		      	  if(g1.equals("Thriller"))
		      	  {
		      	  	count3++;
		      	  }
		      }
		      if(count3==2)
		      {
		      	  //context.write(new Text(tuple_array[0]),one); 
		      	  System.out.println(tuple_array[0] + " ; " +tuple_array[2] + " ; " +tuple_array[4]);
		      	  String intermidiateKey = "[2011-2015]:Action,Thriller";
			  word3.set(intermidiateKey);
			  context.write(word3, one);
			  count3 =0;
		      }
	      }
	    }      
      }
      /*if(genre.contains("Romance"))
      {
    	  //String str = tuple_array[0] + "--" +tuple_array[1] + "--" + tuple_array[2] + "--" + tuple_array[3] + "--" +tuple_array[4];
    	  context.write(new Text(tuple_array[0]),one); 
      }*/
    }
  }
  public static class TokenizerMapper3
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word1 = new Text();
    private Text word2 = new Text();
    private Text word3 = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
FileSplit fileSplit = (FileSplit)context.getInputSplit();
String filename = fileSplit.getPath().getName();

      /*StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }*/
      //initially if the genre is %Romance% and %comedy% - 5th string and  year is between 2001-2005 - 4th str filter
      //if the condition is true the write movie, one
      String tuple = value.toString();
      String[] tuple_array = tuple.split(";");
     // String match genre = "%Comedy%Romance%";
      String genre = tuple_array[4];
      String m = tuple_array[1];
      String g[] = genre.split(",");
      int count1 =0;
      int count2 =0;
      int count3 =0;
      int lb1 = Integer.parseInt("2001");
      int ub1 = Integer.parseInt("2005");
      int lb2 = Integer.parseInt("2006");
      int ub2 = Integer.parseInt("2010");
      int lb3 = Integer.parseInt("2011");
      int ub3 = Integer.parseInt("2015");
      String test = tuple_array[3];
      if(tuple_array.length == 5 && !test.equals("\\N"))
      {
      	     int test_year = Integer.parseInt(test);
	     if(test_year >=lb1 && test_year<=ub1 && m.equals("movie"))
	     {
	      for(String g1 : g)
	      {
		      if(!g1.equals("\\N"))
		      {
		      	  if(g1.equals("Adventure"))
		      	  {
		      	  	count1++;
		      	  }
		      	  if(g1.equals("Sci-Fi"))
		      	  {
		      	  	count1++;
		      	  }
		      }
		      if(count1==2)
		      {
		      	  //context.write(new Text(tuple_array[0]),one); 
		      	  System.out.println(tuple_array[0] + " ; " +tuple_array[2] + " ; " +tuple_array[4]);
		      	  String intermidiateKey = "[2001-2005]:Adventure,Sci-Fi";
			  word1.set(intermidiateKey);
			  context.write(word1, one);
			  count1 =0;
		      }
	      }
	    }      
      }
      if(tuple_array.length == 5 && !test.equals("\\N"))
      {
      	     int test_year = Integer.parseInt(test);
	     if(test_year >=lb2 && test_year<=ub2 && m.equals("movie"))
	     {
	      for(String g1 : g)
	      {
		      if(!g1.equals("\\N"))
		      {
		      	  if(g1.equals("Adventure"))
		      	  {
		      	  	count2++;
		      	  }
		      	  if(g1.equals("Sci-Fi"))
		      	  {
		      	  	count2++;
		      	  }
		      }
		      if(count2==2)
		      {
		      	  //context.write(new Text(tuple_array[0]),one); 
		      	  System.out.println(tuple_array[0] + " ; " +tuple_array[2] + " ; " +tuple_array[4]);
		      	  String intermidiateKey = "[2006-2010]:Adventure,Sci-Fi";
			  word2.set(intermidiateKey);
			  context.write(word2, one);
			  count2 =0;
		      }
	      }
	    }      
      }
      if(tuple_array.length == 5 && !test.equals("\\N"))
      {
      	     int test_year = Integer.parseInt(test);
	     if(test_year >=lb3 && test_year<=ub3 &&  m.equals("movie"))
	     {
	      for(String g1 : g)
	      {
		      if(!g1.equals("\\N"))
		      {
		      	  if(g1.equals("Adventure"))
		      	  {
		      	  	count3++;
		      	  }
		      	  if(g1.equals("Sci-Fi"))
		      	  {
		      	  	count3++;
		      	  }
		      }
		      if(count3==2)
		      {
		      	  //context.write(new Text(tuple_array[0]),one); 
		      	  System.out.println(tuple_array[0] + " ; " +tuple_array[2] + " ; " +tuple_array[4]);
		      	  String intermidiateKey = "[2011-2015]:Adventure,Sci-Fi";
			  word3.set(intermidiateKey);
			  context.write(word3, one);
			  count3 =0;
		      }
	      }
	    }      
      }
      /*if(genre.contains("Romance"))
      {
    	  //String str = tuple_array[0] + "--" +tuple_array[1] + "--" + tuple_array[2] + "--" + tuple_array[3] + "--" +tuple_array[4];
    	  context.write(new Text(tuple_array[0]),one); 
      }*/
    }
  }
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) 
      {
        //sum += val.get();
        sum += val.get();
      }
      result.set(sum);
      String strArr[] = key.toString().split(":");
      if (strArr.length > 1) 
      {
      String year = strArr[0];
      String genre = strArr[1];

      String newKey = year + "," + genre + "-";
      key.set(newKey);
      }
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
   /* Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMappe r1.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);*/
    //
     	Path one = new Path(args[1]);
        Path two = new Path(args[2]);
        Path three = new Path(args[3]);

        //Path three = new Path(args[2]);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper1.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        if (fs.exists(one))
            fs.delete(one, true);
        FileOutputFormat.setOutputPath(job, one);
        job.waitForCompletion(true);
        //another mapper
        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(TokenizerMapper2.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        if (fs.exists(two))
            fs.delete(two, true);
        FileOutputFormat.setOutputPath(job1, two);
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "word count");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(TokenizerMapper3.class);
        job2.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(IntSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        if (fs.exists(three))
            fs.delete(three, true);
        FileOutputFormat.setOutputPath(job2, three);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
