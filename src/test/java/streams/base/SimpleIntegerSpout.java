package streams.base;


import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.*;

public class SimpleIntegerSpout extends BaseRichSpout {

    private int tupleDimensionSize = 5;

    Random rand = new Random();

    private int integerLimit = 25;

    private Long  messageIdCounter = 0L;

    private SpoutOutputCollector _collector;

    public SimpleIntegerSpout(int tupleDimensionSize) {
        this.tupleDimensionSize = tupleDimensionSize;
    }

    public SimpleIntegerSpout(int tupleDimensionSize, int upperLimitForRandomness) {
        this.tupleDimensionSize = tupleDimensionSize;
        this.integerLimit = upperLimitForRandomness;
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
        for (int i=1; i <= tupleDimensionSize;i++) {
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
        Utils.sleep(100);
        Integer[] intArray = new Integer[tupleDimensionSize];
        for ( int i=0; i < tupleDimensionSize; i++) {
            intArray[i]=rand.nextInt(integerLimit);
        }
        _collector.emit(new Values(intArray),messageIdCounter++);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        return ret;
    }
}
