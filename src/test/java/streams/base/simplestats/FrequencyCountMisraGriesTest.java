package streams.base.simplestats;


import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.*;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) {
                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout("1", new TestWordSpout(true), 3);
                builder.setBolt("2", new TestWordCounter(), 4).fieldsGrouping(
                        "1", new Fields("word"));
                builder.setBolt("3", new TestGlobalCount()).globalGrouping("1");
                builder.setBolt("4", new TestAggregatesCounter())
                        .globalGrouping("2");
                StormTopology topology = builder.createTopology();

                // complete the topology

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("1", new Values("nathan"),
                        new Values("bob"), new Values("joey"), new Values(
                        "nathan"));

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

                // check whether the result is right
                assertTrue(Testing.multiseteq(new Values(new Values("nathan"),
                        new Values("bob"), new Values("joey"), new Values(
                        "nathan")), Testing.readTuples(result, "1")));
                assertTrue(Testing.multiseteq(new Values(new Values("nathan", 1),
                        new Values("nathan", 2), new Values("bob", 1),
                        new Values("joey", 1)), Testing.readTuples(result, "2")));
                assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2),
                        new Values(3), new Values(4)), Testing.readTuples(
                        result, "3")));
                assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2),
                        new Values(3), new Values(4)), Testing.readTuples(
                        result, "4")));
            }

        });


    }

}
