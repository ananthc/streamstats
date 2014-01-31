package streams.base.sketches;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import junit.framework.TestCase;
import org.junit.Test;
import streams.base.SimpleCompositeSpout;
import streams.base.bolts.CountMinSketchBolt;
import streams.base.hashtypes.CompositeHashDataType;
import streams.base.hashtypes.CompositeHasherFactory;
import streams.base.simplestats.InvalidConfigException;

import java.util.HashSet;

public class CountMinSketchTest extends TestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testCounts(){

        TopologyBuilder builder = new TopologyBuilder();
        HashSet<Integer> stringIndexes = new HashSet<Integer>();
        stringIndexes.add(1); //2nd tuple element is a string
        stringIndexes.add(4); // last element is a string
        CompositeHashDataType[] dataTypeInfo = new CompositeHashDataType[] {
                CompositeHashDataType.INTEGER,
                CompositeHashDataType.STRING,
                CompositeHashDataType.INTEGER,
                CompositeHashDataType.INTEGER,
                CompositeHashDataType.STRING
        };

        builder.setSpout("spout", new SimpleCompositeSpout(stringIndexes,5,20), 1);
        CompositeHasherFactory intFirstHasherFactory = new CompositeHasherFactory(new String[]{"0","1","2","3","4"},dataTypeInfo,20,30);

        try {
            // count first col hinged on the 2nd col - i.e. all elements in the 2nd col
            //builder.setBolt("bolt", new FrequenctItemsMGBolt<Integer>(3,"1"),1).globalGrouping("spout");
            //builder.setBolt("bolt", new DistinctCountsBJKSTBolt<CompositeHasherFactory>(intFirstHasherFactory,
              //      intSecondHasherFactory,20,25,20),1).globalGrouping("spout");
            // count fifth col hinged on the 3rd column
            //builder.setBolt("freqCountsForAllElementsOccuringInCol3", new FrequencyCountsMisraGries(3,"2",3)).fieldsGrouping("1", new Fields("3"));
            builder.setBolt("countminsketch", new CountMinSketchBolt<CompositeHasherFactory>(intFirstHasherFactory,0.02f,0.00001f))
                    .globalGrouping("spout");
        } catch (InvalidConfigException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testTopology", conf, builder.createTopology());
        Utils.sleep(300000);
        cluster.killTopology("testTopology");
        cluster.shutdown();



    }

}
