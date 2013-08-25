package streams.base;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.*;

public class SimpleCompositeSpout extends BaseRichSpout {


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

    private int integerLimit = 25;

    private Long  messageIdCounter = 0L;

    private List<Boolean> stringIndexes = new ArrayList<Boolean>();

    public SimpleCompositeSpout(Set<Integer> stringColumnIndexes, int dimension, int upperLimitForIntRandomness) {
        integerLimit = upperLimitForIntRandomness;
        tupleDimensionSize = dimension;
        if (stringColumnIndexes != null) {
            for (int i=0; i < dimension; i++) {
                if (stringColumnIndexes.contains(i))
                    stringIndexes.add(Boolean.TRUE);
                else
                    stringIndexes.add(Boolean.FALSE);
            }
        }
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
        Utils.sleep(100);
        Values returnVal = new Values();
        for (int i = 0; i < tupleDimensionSize; i++) {
            if (stringIndexes.get(i))
                returnVal.add((i),randomStrings[rand.nextInt(randomStrings.length)]);
            else
                returnVal.add((i),rand.nextInt(integerLimit));
        }


        _collector.emit(returnVal,messageIdCounter++);

    }
}
