package streams.base.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import streams.base.hashtypes.BaseHasherFactory;
import streams.base.simplestats.BJKSTDistinctElements;
import streams.base.simplestats.InvalidConfigException;
import streams.base.simplestats.InvalidDataException;

import java.util.Map;

public class DistinctCountsBJKSTBolt<T extends BaseHasherFactory> extends BaseRichBolt {


    private OutputCollector collector;


    private boolean anchorTuple = true;

    private boolean strictMode = false;

    private BJKSTDistinctElements<T> distinctsCounter;

    public DistinctCountsBJKSTBolt(T firstHasher , T seondaryHasher ,int numberOfMedianAttempts ,
                                   int sizeOfEachMedianSet, int secondaryHashSizeFactor) throws InvalidConfigException {
        distinctsCounter = new BJKSTDistinctElements<T>(firstHasher,seondaryHasher,numberOfMedianAttempts,
                sizeOfEachMedianSet,secondaryHashSizeFactor);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector =  outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (anchorTuple)
                collector.emit(tuple,distinctsCounter.processTuple(tuple) );
            else
                collector.emit(distinctsCounter.processTuple(tuple));
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
        outputFieldsDeclarer.declare(new Fields("distinctCount"));
    }
}
