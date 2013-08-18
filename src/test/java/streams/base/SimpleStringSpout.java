package streams.base;


import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.*;

public class SimpleStringSpout extends BaseRichSpout {

    private int tupleDimensionSize = 5;

    private Random rand = new Random();

    private SpoutOutputCollector _collector;

    private String[] randomStrings = {
                                        "one","two","three","four","five","six","seven","eight","nine","ten","eleven",
                                        "tweleve","thirteen","fourteen","fifteen","sixteen","seventeen",
                                        "eighteen","nineteen","twenty","twenty one","twenty two","twenty three",
                                        "twenty four","Two roads", "diverged in", "a yellow wood" ,
                                        "And sorry" ,  "I could not travel" ,  "both" ,
                                        "And be one traveler, long I stood","And looked down one as far as I could",
                                        "To where it bent in the undergrowth"

                                     };

    public SimpleStringSpout(int tupleDimensionSize) {
        this.tupleDimensionSize = tupleDimensionSize;

    }

    public int getTupleDimensionSize() {
        return tupleDimensionSize;
    }

    public void setTupleDimensionSize(int tupleDimensionSize) {
        this.tupleDimensionSize = tupleDimensionSize;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        List<String> fieldNames = new ArrayList<String>();
        for (int i=0; i < tupleDimensionSize;i++) {
            fieldNames.add(new String(""+i));
        }
        outputFieldsDeclarer.declare(new Fields(fieldNames.toArray(new String[0]) ));
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        String[] stringArray = new String[tupleDimensionSize];
        for ( int i=0; i < tupleDimensionSize; i++) {
            stringArray[i]=randomStrings[rand.nextInt(randomStrings.length)];
        }
        _collector.emit(new Values(stringArray));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        return ret;
    }
}

