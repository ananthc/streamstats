package streams.base.bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import streams.base.hashtypes.BaseHasherFactory;
import streams.base.simplestats.InvalidConfigException;
import streams.base.simplestats.InvalidDataException;
import streams.base.sketches.CountMinSketch;

import java.util.Map;

public class CountMinSketchBolt<T extends BaseHasherFactory> extends BaseRichBolt {

    private OutputCollector collector;

    private boolean strictMode = true;

    private boolean anchorTuple = false;

    private CountMinSketch sketch;


    public CountMinSketchBolt(T hasherFactory, float errorFactor, float probabilityOfErrror,
                              streams.base.simplestats.CountSpecifier countSpecifier) throws InvalidConfigException {
        hasherFactory.setNumBins(CountMinSketch.computeNumColumns(errorFactor));
        sketch = new CountMinSketch<T>(hasherFactory,errorFactor,probabilityOfErrror,countSpecifier);
    }

    public CountMinSketchBolt(T hasherFactory, float errorFactor, float probabilityOfErrror) throws InvalidConfigException {
        hasherFactory.setNumBins(CountMinSketch.computeNumColumns(errorFactor));
        sketch = new CountMinSketch<T>(hasherFactory,errorFactor,probabilityOfErrror);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector =  outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (anchorTuple)
                collector.emit(tuple,sketch.processTuple(tuple) );
            else
                collector.emit(sketch.processTuple(tuple));
        } catch (InvalidDataException e) {
            if (strictMode)
                collector.fail(tuple);
            e.printStackTrace();
            return;
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("count"));
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public void setStrictMode(boolean strictMode) {
        this.strictMode = strictMode;
    }

    public boolean isAnchorTuple() {
        return anchorTuple;
    }

    public void setAnchorTuple(boolean anchorTuple) {
        this.anchorTuple = anchorTuple;
    }
}
