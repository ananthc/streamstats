package streams.base.simplestats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FrequencyCountsMisraGries<T> extends BaseRichBolt {
	
	private OutputCollector _collector;
	private int mapMaxSize =1;
	private String filedName = null;
	private int topCountsToOutput = 1;
	private TreeMap<Integer,HashSet<T>> valueBasedMap = new TreeMap<Integer,HashSet<T>>();
	private Map<T,Integer> keyBasedMap = new HashMap<T,Integer>();
	private ObjectMapper mapper = new ObjectMapper();
	
    public FrequencyCountsMisraGries(int topN, String fieldNameToUseForFrequencyCounts, int topOutputCounts) {
    	mapMaxSize = topN;
    	filedName = fieldNameToUseForFrequencyCounts;
    	topCountsToOutput = topOutputCounts;
    }
    
    
    

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
    }


    public void execute(Tuple tuple) {
    	T value = (T)tuple.getValueByField(filedName);
    	if (keyBasedMap.containsKey(value) ){
    		Integer newIntCount = keyBasedMap.get(value) + 1;
    		keyBasedMap.put(value, newIntCount);
    		if (valueBasedMap.containsKey(newIntCount-1)) {
    			HashSet<T> tupleKeys = valueBasedMap.get(newIntCount-1);
    			tupleKeys.remove(value);	
    		}
    		if (valueBasedMap.containsKey(newIntCount)) {
				valueBasedMap.get(newIntCount).add(value);
			}
			else {
				HashSet<T> newSet = new HashSet<T>();
				newSet.add(value);
				valueBasedMap.put(newIntCount, newSet);
			}
    	}
    	else {
    		if (keyBasedMap.size() < mapMaxSize -1) {
    			keyBasedMap.put(value, 1);
    			if (valueBasedMap.containsKey(1)) {
    				valueBasedMap.get(1).add(value);
    			}
    			else {
    				HashSet<T> newSet = new HashSet<T>();
    				newSet.add(value);
    				valueBasedMap.put(1, newSet);
    			}
    		}
    		else {
    			List<T> toBeRemovedList = new ArrayList<T>();
    			for (T aKey: keyBasedMap.keySet()) {
    				Integer newIntCount = keyBasedMap.get(value) - 1;
    				keyBasedMap.put(value, newIntCount);
    				if (valueBasedMap.containsKey(newIntCount+1)) {
    	    			HashSet<T> tupleKeys = valueBasedMap.get(newIntCount+1);
    	    			tupleKeys.remove(value);	
    	    		}
    				if ( newIntCount == 0) {
    					toBeRemovedList.add(value);
    				}
    				else {
    					if (valueBasedMap.containsKey(newIntCount)) {
        					valueBasedMap.get(newIntCount).add(value);
        				}
        				else {
        					HashSet<T> newSet = new HashSet<T>();
        					newSet.add(value);
        					valueBasedMap.put(newIntCount, newSet);
        				}	
    				}
    			}
    			for ( T toRemove : toBeRemovedList) {
    				keyBasedMap.remove(toRemove);
    			}
    		}
    	}
    	Values toPushFurther = new Values();
    	Map<Integer,HashSet<T>> finalOrder = valueBasedMap.descendingMap();
    	Set<Entry<Integer,HashSet<T>>> collectionOfCounts = finalOrder.entrySet();
    	Iterator<Entry<Integer,HashSet<T>>> collectionOfCountsItr = collectionOfCounts.iterator();
    	List<Entry<Integer,HashSet<T>>> jsonListRepresentation = new ArrayList<Entry<Integer,HashSet<T>>>();
    	int counter = 1;
    	while (collectionOfCountsItr.hasNext()) {
    		Entry<Integer,HashSet<T>> anEntry = collectionOfCountsItr.next();
    		jsonListRepresentation.add(anEntry);
    		counter = counter + 1;
    		if ( counter == topCountsToOutput) break;
    	}
    	HashMap<String,Object> jsonFinalVlaue = new HashMap<String,Object>();
    	jsonFinalVlaue.put("count", counter);
    	jsonFinalVlaue.put("topN", jsonListRepresentation);
    	String valueToWrite = null;
    	try {
    		valueToWrite = mapper.writeValueAsString(jsonFinalVlaue);
    		toPushFurther.add(valueToWrite);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
			toPushFurther.add("{}");
		} catch (JsonMappingException e) {
			e.printStackTrace();
			toPushFurther.add("{}");
		} catch (IOException e) {
			e.printStackTrace();
			toPushFurther.add("{}");
		}
    	_collector.emit(tuple, toPushFurther);
    	_collector.ack(tuple);
    }
    

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//    	StringBuffer bffr = new StringBuffer("topN");
//    	for (int i=0;i < topCountsToOutput;i++) {
//    		bffr.append(","+ Integer.toString(i));
//    	}	
    	declarer.declare(new Fields("topN"));
    }    
    

}
