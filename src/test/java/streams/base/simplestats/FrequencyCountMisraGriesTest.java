package streams.base.simplestats;


import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import streams.base.SimpleIntegerSpout;

import java.util.Map;


public class FrequencyCountMisraGriesTest extends TestCase {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void testFrequencyCounts(){
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);
        Testing.withLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) {
                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout("1", new SimpleIntegerSpout(8), 3);
                //try {
                    // count first col hinged on the 2nd col - i.e. all elements in the 2nd col
                    //builder.setBolt("freqCountsForAllElementsOccuringInCol2", new FrequencyCountsMisraGries(3,"1")).fieldsGrouping("1", new Fields("2"));
                    // count fifth col hinged on the 3rd column
                    //builder.setBolt("freqCountsForAllElementsOccuringInCol3", new FrequencyCountsMisraGries(3,"5")).fieldsGrouping("1", new Fields("3"));
                //} catch (InvalidConfigException e) {
                //    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                //}
                StormTopology topology = builder.createTopology();

                // complete the topology

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);
                /**
                 * TODO
                 */
                Map result = Testing.completeTopology(cluster, topology,
                        completeTopologyParam);


            }

        });


    }

}
