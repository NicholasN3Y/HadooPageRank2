package assign2.cs4225;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRankerV2 {
	// record the number of nodes;
	public static int count = 0;
	public static double dampening_factor = 0.8;
	public final static int iterations = 15;
	public final static String assign2Path = "a0112224"+Path.SEPARATOR+"assignment_2";

	// Compute one iteration of PageRank.
	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// parse an input line into page, pagerank, outgoing links 
			String[] input = value.toString().split("\t", 2);
			String page = input[0];
			String[] input2 = input[1].split("\t", 2);
			String pagerank = input2[0];
			String outgoingLinks = "";
			if (input2.length == 2){ 
				outgoingLinks = input2[1];
			}

			// We need to output both graph structure and the credit sent to
			// links
			// Graph structure: output a pair of (page, “EDGE: ”+outgoing links)
			outputKey.set(page);
			outputValue.set("EDGE:\t" + outgoingLinks);
			context.write(outputKey, outputValue);
			System.out.println("edge! "+outputKey + " " + outputValue);

			// Credit: for each outgoing link, output a pair (link,
			// pagerank/number of outgoing links)
			if (outgoingLinks.equals("")){
				;
			}else{
				String[] outgoingNodes = outgoingLinks.split("\t");
				for (String a : outgoingNodes){
					String[] link_win_wout = a.split(" ",3);
					String link = link_win_wout[0];
					double win = Double.parseDouble(link_win_wout[1]);
					double wout = Double.parseDouble(link_win_wout[2]);
					String credit = String.valueOf(Double.parseDouble(pagerank) * win * wout); 
					outputValue.set(credit);	
					outputKey.set(link);
					context.write(outputKey, outputValue);
					System.out.println("credit! " + outputKey + " " + outputValue);
				}
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> value, Context context)

				throws IOException, InterruptedException {
			String outgoingLinks = "";
			double S = 0.0;
			Text outputKey = new Text();
			Text outputValue = new Text();
			Iterator<Text> itr = value.iterator();
			while(itr.hasNext()){
				String input = itr.next().toString();

				String[] inputArr = input.split("\t",2);
				if (inputArr[0].equals(new String("EDGE:"))){
					// analyze values, if the value starts with “EDGE:”, then the phrase
					// after “EDGE:” are outgoing links
					outgoingLinks = inputArr[1].trim();
				}else{
					// sum up the values that do not start with “EDGE:” into a variable
					// S
					S += Double.parseDouble(input);
				}
			}
			// compute new pagerank as (1-d) + (d*S)
			// output (key, newpagerank + outgoing links)
			// int N = context.getConfiguration().getInt("count", 1);
			double pagerank = (1 - dampening_factor) + (dampening_factor * S);
			outputKey.set(key);
			if (outgoingLinks.equals("")){
				outputValue.set(String.valueOf(pagerank));
			}else{	
				outputValue.set(pagerank + "\t" + outgoingLinks);
			}
			context.write(outputKey, outputValue);
			System.out.println(outputKey + "\t" + outputValue);
		}
	}

	public static class Mapper2 extends Mapper<Object, Text, DoubleWritable, Text> {

		private DoubleWritable outputKey = new DoubleWritable();
		private Text outputValue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] input = value.toString().split("\t", 2);
			String credit = input[1].split("\t", 2)[0];
			outputKey.set(Double.valueOf(credit));
			outputValue.set(input[0]);
			context.write(outputKey, outputValue);
		}
	}

	public static class Reducer2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		private Text outputValue = new Text();

		@Override
		public void reduce(DoubleWritable key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> itr = value.iterator();
			ArrayList<Long> pages = new ArrayList<Long>();
			while (itr.hasNext()){
				Long page = Long.valueOf(itr.next().toString());
				pages.add(page);
			}
			Object[] pagesArr = pages.toArray();
			Arrays.sort(pagesArr);
			String output = "";
			for(Object page : pagesArr){
				output += page.toString() + " ";
			}
			outputValue.set(output.trim());
			context.write(key, outputValue);
		}
	}

	@SuppressWarnings("rawtypes")
	public static class KeyDescendingComparator extends WritableComparator {
		protected KeyDescendingComparator(){
			super(DoubleWritable.class, true);
		}

		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			DoubleWritable key1 = (DoubleWritable) o1;
			DoubleWritable key2 = (DoubleWritable) o2;

			if (key1.get() < key2.get()){
				return 1;
			}else if(key1.get() == key2.get()){
				return 0;
			}else{
				return -1;
			}
		}
	}

	// Count the number of nodes and attach a pagerank(pr) of  score 1	
	// reformat the file to: nodeA + "\t" + pr [+ "\t"+"nodeA: "+Win(A,B)+" "+Wout(A.B)]* 
	public static void preprocessing(String filename) 
			throws FileNotFoundException, IOException{
		FileSystem fs = FileSystem.get(new Configuration());

		Scanner sc = new Scanner(new InputStreamReader(fs.open(new Path(filename))));
		sc.useDelimiter("\n");
		Path filepath = new Path(filename);
		String name = filepath.getName();
		String processedFpath = assign2Path + Path.SEPARATOR + "inputV2" + Path.SEPARATOR + name;
		FSDataOutputStream fsOS = fs.create(new Path(processedFpath));
		String line;
		Map<String, ArrayList<String>> inLinks_map = new HashMap<String, ArrayList<String>>();
		Map<String, ArrayList<String>> outLinks_map = new HashMap<String, ArrayList<String>>();
		String node;
		String[] nodeLinks;
		//parse each line in the file
		while (sc.hasNext()){
			line=sc.next();
			count ++;
			ArrayList<String> outLinks = new ArrayList<String>();
			nodeLinks = line.split("\t", 2);
			node = nodeLinks[0].trim();
			if (nodeLinks.length==1){
				continue;
			}
			for (String v : nodeLinks[1].split("\t")){
				//inverse mapping v->node
				if(inLinks_map.containsKey(v)){
					inLinks_map.get(v).add(node);
				}else{
					ArrayList<String> parent = new ArrayList<String>();
					parent.add(node);
					inLinks_map.put(v, parent);
				}
				outLinks.add(v.trim());
			}
			outLinks_map.put(node, outLinks);
		}


		for (String v : outLinks_map.keySet()){
			String output = v+"\t1.0";
			for (String u : outLinks_map.get(v)){
				double win = computeWin(v, u, inLinks_map, outLinks_map);
				double wout = computeWout(v, u, outLinks_map);
				output+="\t"+u+" "+win+" "+wout;
			}
			fsOS.write(output.getBytes());
			fsOS.write("\r\n".getBytes());
		}

		sc.close();
		fsOS.close();
		fs.close();
	}

	private static double computeWout(String v, String u,
			Map<String, ArrayList<String>> outLinks_map) {

		ArrayList<String> v_out = null;
		v_out = outLinks_map.get(v);

		if(v_out == null){ return 0;}

		double denom = 0;
		double numer = 0;
		for (String node : v_out){
			ArrayList<String> u_out = outLinks_map.get(node);
			if (u_out != null){
				denom += u_out.size();
				if (node.equals(u)){
					numer = u_out.size();
				}
			}
		}
		if (denom == 0){return 0.00001;}
		return (numer / denom);
	}

	private static double computeWin(String v, String u, Map<String, ArrayList<String>> inLinks_map,
			Map<String, ArrayList<String>> outLinks_map) {

		ArrayList<String> v_out = null;
		v_out = outLinks_map.get(v);

		if(v_out == null){ return 0;}

		double denom = 0;
		double numer = 0;
		for (String node : v_out){
			ArrayList<String> u_in = inLinks_map.get(node);
			if (u_in != null){
				denom += u_in.size();
				if (node.equals(u)){
					numer = u_in.size();
				}
			}
		}
		//avoid division by zero
		if (denom == 0){ return 0.00001;}
		return (numer / denom);

	}

	public static void main(String[] args) throws IOException,
	InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: PageRank <input1>");
			System.exit(2);
		}

		String input = otherArgs[0];
		preprocessing(input);
		String inputFile = assign2Path+Path.SEPARATOR+"inputV2";
		String tmpOutput = assign2Path+Path.SEPARATOR+"outputV2";
		for (int i = 0; i < iterations; i++) {
			// create a new job, set job configurations and run the job
			Job job = Job.getInstance(conf, "PageRankIteration"+(i+1));
			conf.setInt("count", count);
			job.setJarByClass(PageRankerV2.class);
			job.setMapperClass(Mapper1.class);
			job.setReducerClass(Reducer1.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputFile));
			FileOutputFormat.setOutputPath(job, new Path(tmpOutput));
			job.waitForCompletion(true);

			//reset output to input before the next iteration starts
			//if (i != iterations-1){
			FileSystem fs = FileSystem.get(conf);

			Path tmp = new Path(tmpOutput+Path.SEPARATOR+"part-r-00000");
			Path in = new Path(inputFile+Path.SEPARATOR+new Path(input).getName());
			boolean success = false;
			while (!success){
				//delete input for this iteration
				success = fs.delete(in, true);
			}
			success = false;
			while(!success){
				//set output of this iteration to input of next iteration
				success = fs.rename(tmp,in);
			}
			success = false;
			while(!success){
				//delete output folder of this iteration
				success = fs.delete(new Path(tmpOutput),true);
			}
			fs.close();
			//}
		}
		//sort the results
		Job job = Job.getInstance(conf, "PageRankResultSort");
		job.setJarByClass(PageRankerV2.class);
		job.setMapperClass(Mapper2.class);
		job.setSortComparatorClass(KeyDescendingComparator.class);
		job.setReducerClass(Reducer2.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(tmpOutput));
		job.waitForCompletion(true);
	}
}
