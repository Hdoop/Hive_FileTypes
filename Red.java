package mr.lib.code;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Red extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
	private Map<Text, DoubleWritable> countMap = new HashMap<>();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        // computes the number of occurrences of a single word
        double sum = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }

        // puts the number of occurrences of this word into the map.
        // We need to create another Text object because the Text instance
        // we receive is the same for all the words
        countMap.put(new Text(key), new DoubleWritable(sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Map<Text, DoubleWritable> sortedMap = sortByValues(countMap);

        int counter = 0;
        for (Text key : sortedMap.keySet()) {
            if (counter++ == 10) {
                break;
            }
            context.write(key, sortedMap.get(key));
        }
    }


/**
 * The combiner retrieves every word and puts it into a Map: if the word already exists in the
 * map, increments its value, otherwise sets it to 1.
 */

/*public static class TopNCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        // computes the number of occurrences of a single word
        int sum = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        context.write(key, new DoubleWritable(sum));
    }
}
*/
/*
* sorts the map by values. Taken from:
* http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
*/
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
	    List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
	
	    Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
	
	        @Override
	        public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
	            return o2.getValue().compareTo(o1.getValue());
	        }
	    });
	
	    //LinkedHashMap will keep the keys in the order they are inserted
	    //which is currently sorted on natural ordering
	    Map<K, V> sortedMap = new LinkedHashMap<K, V>();
	
	    for (Map.Entry<K, V> entry : entries) {
	        sortedMap.put(entry.getKey(), entry.getValue());
	    }
	
	    return sortedMap;
	}

}
