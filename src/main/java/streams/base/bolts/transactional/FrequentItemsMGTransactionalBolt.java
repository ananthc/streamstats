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
import streams.base.simplestats.FrequencyCountsMisraGries;
import streams.base.simplestats.InvalidConfigException;
import streams.base.simplestats.InvalidDataException;

import java.util.HashMap;
import java.util.Map;


public class FrequentItemsMGTransactionalBolt<T> extends BaseTransactionalBolt
    implements ICommitter {
    private BatchOutputCollector collector;
    private TransactionAttempt currentTransaction;

    private TransactionValueManager valueManager;
    private HashMap valueManagerInfo;

    private FrequencyCountsMisraGries<T> counter;

    private Values lastKnownFrequencyCounts;

    private boolean strictMode = false;


    public FrequentItemsMGTransactionalBolt(int bucketSize, String fieldNameToUseForFrequencyCounts,
                                            HashMap valueManagerInfo,TransactionValueManager valueManager) throws InvalidConfigException {
        this.valueManager = valueManager;
        this.valueManagerInfo = valueManagerInfo;
        this.counter = new FrequencyCountsMisraGries<T>(bucketSize,fieldNameToUseForFrequencyCounts);
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
            lastKnownFrequencyCounts= counter.processTuple(tuple);
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
            valueManager.persistValues(valueManagerInfo,lastKnownFrequencyCounts,currentTransaction.getTransactionId());
        }
        collector.emit(new Values(currentTransaction.getTransactionId(), lastKnownFrequencyCounts));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("frequentItems"));
    }
}
