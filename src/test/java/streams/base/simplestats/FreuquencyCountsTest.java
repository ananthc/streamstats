package streams.base.simplestats;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import streams.base.SimpleIntegerSpout;
import streams.base.bolts.FrequenctItemsMGBolt;

public class FreuquencyCountsTest {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SimpleIntegerSpout(3,100), 1);
        try {
            // count first col hinged on the 2nd col - i.e. all elements in the 2nd col
            builder.setBolt("bolt", new FrequenctItemsMGBolt<Integer>(3,"1"),1).globalGrouping("spout");
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
