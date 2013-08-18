package streams.base.simplestats;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FrequencyCountsMisraGries<T>  {

	protected int mapMaxSize =1;
	protected String fieldName = null;

	private Map<T,Integer> keyBasedMap = new HashMap<T,Integer>();

	
    public FrequencyCountsMisraGries(int bucketSize, String fieldNameToUseForFrequencyCounts)
                                                                            throws InvalidConfigException {
        mapMaxSize = bucketSize;
    	fieldName = fieldNameToUseForFrequencyCounts;
    }
    
    

    public Values processTuple(Tuple tuple) throws InvalidDataException  {
        Values result = new Values();
        if (!tuple.contains(fieldName)) {
            throw new InvalidDataException("Tuple Does not contain the field " + tuple.toString());
        }
        T value = (T)tuple.getValueByField(fieldName);
        Integer newIntCount = 0;
        if (keyBasedMap.containsKey(value) ){
            newIntCount= keyBasedMap.get(value) + 1;
    		keyBasedMap.put(value, newIntCount);
    	}
    	else {
    		if (keyBasedMap.size() < mapMaxSize -1) {
                keyBasedMap.put(value, 1);
    		}
    		else {
                Iterator<Map.Entry<T,Integer>> itr = keyBasedMap.entrySet().iterator();
                while (itr.hasNext()) {
                    Map.Entry<T,Integer> entry = itr.next();
                    newIntCount = keyBasedMap.get(entry.getKey()) - 1;
                    if (newIntCount <=0 ) {
                        itr.remove();
                    }
                    else {
                        keyBasedMap.put(entry.getKey(), newIntCount);
                    }
    		    }
    	    }
        }

    	HashMap<String, Object> jsonFinalVlaue = new HashMap<String,Object>();
    	jsonFinalVlaue.put("count", keyBasedMap.size());
    	jsonFinalVlaue.put("topN", keyBasedMap);
    	String valueToWrite = null;
        ObjectMapper mapper = new ObjectMapper();
    	try {
    		valueToWrite = mapper.writeValueAsString(jsonFinalVlaue);
            result.add(valueToWrite);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
            result.add("{}");
		} catch (JsonMappingException e) {
			e.printStackTrace();
            result.add("{}");
		} catch (IOException e) {
			e.printStackTrace();
            result.add("{}");
		}
        return result;

    }



}
