import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer
		extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private Text wordResult = new Text();
	private IntWritable countResult = new IntWritable();
	private HashMap<Text, Integer> countTable = new HashMap<Text, Integer>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		countTable.put(key, sum);
	}
	
	@Override
	public void cleanup(Context context)
            throws IOException, InterruptedException {
		
		List<Entry<Text, Integer>> countList = new LinkedList<Entry<Text, Integer>>(countTable.entrySet());
		Collections.sort(countList, new Comparator<Entry<Text, Integer>>() {
			public int compare(Entry<Text, Integer> entry1, Entry<Text, Integer> entry2) {
				return entry2.getValue().compareTo(entry1.getValue());
			}
		});

		int topN = countList.size() < 5 ? countList.size() : 5;
		List<Entry<Text, Integer>> topFiveCountList = countList.subList(0, topN);
		for (Entry<Text, Integer> entry : topFiveCountList) {
			wordResult.set(entry.getKey());
			countResult.set(entry.getValue());
			context.write(wordResult, countResult);
		}
	}
}
