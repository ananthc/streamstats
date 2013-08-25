package streams.base.bolts.transactional;


import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import streams.base.hashtypes.BaseHasherFactory;
import streams.base.simplestats.BJKSTDistinctElements;
import streams.base.simplestats.InvalidConfigException;
import streams.base.simplestats.InvalidDataException;

import java.util.HashMap;
import java.util.Map;

public class DistinctCountsBJKSTTransactionalBolt<T extends BaseHasherFactory> extends BaseTransactionalBolt
        implements ICommitter {

    private BatchOutputCollector collector;
    private TransactionAttempt currentTransaction;

    private TransactionValueManager valueManager;
    private HashMap valueManagerInfo;

    private Values lastKnownDistinctCount;

    private boolean strictMode = false;


    private BJKSTDistinctElements<T> distinctsCounter;

    public DistinctCountsBJKSTTransactionalBolt(T firstHasher , T seondaryHasher ,int numberOfMedianAttempts ,
                                                int sizeOfEachMedianSet, int secondaryHashSizeFactor,
                                                HashMap valueManagerInfo, TransactionValueManager valueManager)
                                                                                    throws InvalidConfigException {
        this.valueManager = valueManager;
        this.valueManagerInfo = valueManagerInfo;
        distinctsCounter = new BJKSTDistinctElements<T>(firstHasher,seondaryHasher,numberOfMedianAttempts,
                sizeOfEachMedianSet,secondaryHashSizeFactor);
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public void setStrictMode(boolean strictMode) {
        this.strictMode = strictMode;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext,
                        BatchOutputCollector batchOutputCollector,
                        TransactionAttempt transactionAttempt) {
        collector = batchOutputCollector;
        currentTransaction = transactionAttempt;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            lastKnownDistinctCount= distinctsCounter.processTuple(tuple);
        } catch (InvalidDataException e) {
            e.printStackTrace();
            if (strictMode)
                collector.reportError(e);
        }
    }

    @Override
    public void finishBatch() {
        Values newValue = null;
        Values bestKnownValue = valueManager.getLastKnownValue(valueManagerInfo);
        if(bestKnownValue == null || ! valueManager.getLastKnownTransactionID(valueManagerInfo).equals(
                currentTransaction.getTransactionId())) {
            valueManager.persistValues(valueManagerInfo,lastKnownDistinctCount,currentTransaction.getTransactionId());
        }
        collector.emit(new Values(currentTransaction.getTransactionId(), lastKnownDistinctCount));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("distinctCount"));
    }
}
