package streams.base.simplestats;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import streams.base.SimpleCompositeSpout;
import streams.base.bolts.DistinctCountsBJKSTBolt;
import streams.base.hashtypes.CompositeHashDataType;
import streams.base.hashtypes.CompositeHasherFactory;

import java.util.HashSet;

public class FreuquencyCountsTest {


    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

//        TopologyBuilder builder = new TopologyBuilder();
//        builder.setSpout("spout", new SimpleIntegerSpout(3,6000), 1);
//        IntegerHasherFactory intFirstHasherFactory = new IntegerHasherFactory("2",20,32);
//        IntegerHasherFactory intSecondHasherFactory = new IntegerHasherFactory("2",20,32);
//        builder.setSpout("spout", new SimpleStringSpout(5), 1);
//        StringHasherFactory intFirstHasherFactory = new StringHasherFactory("2",20,1024);
//        StringHasherFactory intSecondHasherFactory = new StringHasherFactory("2",20,1024);
//
//        try {
//            // count first col hinged on the 2nd col - i.e. all elements in the 2nd col
//            //builder.setBolt("bolt", new FrequenctItemsMGBolt<Integer>(3,"1"),1).globalGrouping("spout");
//            builder.setBolt("bolt", new DistinctCountsBJKSTBolt<StringHasherFactory>(intFirstHasherFactory,
//                    intSecondHasherFactory,20,25,20),1).globalGrouping("spout");
//            // count fifth col hinged on the 3rd column
//            //builder.setBolt("freqCountsForAllElementsOccuringInCol3", new FrequencyCountsMisraGries(3,"2",3)).fieldsGrouping("1", new Fields("3"));
//        } catch (InvalidConfigException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }


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
        CompositeHasherFactory intSecondHasherFactory = new CompositeHasherFactory(new String[]{"0","1","2","3","4"},dataTypeInfo,5,30);

        try {
            // count first col hinged on the 2nd col - i.e. all elements in the 2nd col
            //builder.setBolt("bolt", new FrequenctItemsMGBolt<Integer>(3,"1"),1).globalGrouping("spout");
            builder.setBolt("bolt", new DistinctCountsBJKSTBolt<CompositeHasherFactory>(intFirstHasherFactory,
                    intSecondHasherFactory,20,25,20),1).globalGrouping("spout");
            // count fifth col hinged on the 3rd column
            //builder.setBolt("freqCountsForAllElementsOccuringInCol3", new FrequencyCountsMisraGries(3,"2",3)).fieldsGrouping("1", new Fields("3"));
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
