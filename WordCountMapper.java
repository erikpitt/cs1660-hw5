import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.Comparator;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper
		extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text wordResult = new Text();
	private IntWritable countResult = new IntWritable();
	private HashMap<String, Integer> countTable = new HashMap<String, Integer>();
	private static final HashSet<String> stopWordSet = new HashSet<String>(Arrays.asList("he", "she", "they", "the", "a", "an", "are", "you", "of", "is", "and", "or"));
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer itr = new StringTokenizer(value.toString());

		while (itr.hasMoreTokens()) {
			String word = itr.nextToken();
			if (!stopWordSet.contains(word)) {
				int count = countTable.containsKey(word) ? countTable.get(word) : 0;
				countTable.put(word, count + 1);
			}
		}
	}
	
	@Override
	public void cleanup(Context context)
            throws IOException, InterruptedException {
		
		List<Entry<String, Integer>> countList = new LinkedList<Entry<String, Integer>>(countTable.entrySet());
		Collections.sort(countList, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {
				return entry2.getValue().compareTo(entry1.getValue());
			}
		});

		int topN = countList.size() < 5 ? countList.size() : 5;
		List<Entry<String, Integer>> topFiveCountList = countList.subList(0, topN);
		for (Entry<String, Integer> entry : topFiveCountList) {
			wordResult.set(entry.getKey());
			countResult.set(entry.getValue());
			context.write(wordResult, countResult);
		}
	}
}