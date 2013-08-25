package streams.base.bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import streams.base.simplestats.FrequencyCountsMisraGries;
import streams.base.simplestats.InvalidConfigException;
import streams.base.simplestats.InvalidDataException;

import java.util.Map;

public class FrequenctItemsMGBolt<T> extends BaseRichBolt {


    private OutputCollector collector;

    private FrequencyCountsMisraGries<T> counter;

    private boolean anchorTuple = true;

    private boolean strictMode = false;

    public FrequenctItemsMGBolt(int bucketSize, String fieldNameToUseForFrequencyCounts) throws InvalidConfigException {
        counter = new FrequencyCountsMisraGries<T>(bucketSize,fieldNameToUseForFrequencyCounts);
    }

    public boolean isAnchorTuple() {
        return anchorTuple;
    }

    public void setAnchorTuple(boolean anchorTuple) {
        this.anchorTuple = anchorTuple;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector =  outputCollector;

    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public void setStrictMode(boolean strictMode) {
        this.strictMode = strictMode;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (anchorTuple)
                collector.emit(tuple,counter.processTuple(tuple) );
            else
                collector.emit(counter.processTuple(tuple));
        } catch (InvalidDataException e) {
            if (strictMode)
                collector.fail(tuple);
            e.printStackTrace();
            return;
        }
        collector.ack(tuple);
    }

    @Override

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("frequentItems"));
    }



}
