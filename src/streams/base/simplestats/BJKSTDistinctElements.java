package streams.base.simplestats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class BJKSTDistinctElements<T extends IntegerHasher>  extends BaseRichBolt {
	

	private static final long serialVersionUID = -2032575802259420762L;
	
	private int numberOfBins = 32;
	
	private int w = 32;
	
	private int numMedians=100;
	
	private float error = 0.02f;
	
	private float confidence_relaxation_limit = 0.05f;
	
	private int sizeOfMedianSet;

	T type;
	// make this large enough as possible. This one effects the permissible size of the buffer bins for the secondary hash function
	private int secondaryHashSizeFactor ;
	
	
	// make c as large as possible. This reflects the size of the buffer 
	private int c;
	
	private OutputCollector _collector;
	
	private List<Integer> limits = new ArrayList<Integer>();
	
	private List<HashMap<Integer,Integer>> buffers = new ArrayList<HashMap<Integer,Integer>>();
	
	private List<Universal2Hasher> hHashers = new ArrayList<Universal2Hasher>();
	
	private List<Universal2Hasher> gHashers = new ArrayList<Universal2Hasher>();
	
	public BJKSTDistinctElements(int numberOfBins, int numOfBitsInWord, int numberOfMedianAttempts , 
												int sizeOfEachMedianSet, int secondaryHashSizeFactor )
														throws InvalidConfigException  {
		this.numberOfBins = numberOfBins;
		this.numMedians = numberOfMedianAttempts;
		this.sizeOfMedianSet = sizeOfEachMedianSet;
		this.secondaryHashSizeFactor = secondaryHashSizeFactor;
		init();
	}

	public BJKSTDistinctElements(int numberOfBins, int numberOfMedianAttempts , int sizeOfEachMedianSet, 
			                                       float allowedError, float confidence_relaxation_limit,int secondaryHashSizeFactor )
	               throws InvalidConfigException {
		if (allowedError > 1) {
			throw new InvalidConfigException("Permitted error should be < 1 and in float format");
		}
		this.numberOfBins = numberOfBins;
		this.numMedians = numberOfMedianAttempts;
		this.sizeOfMedianSet = sizeOfEachMedianSet;
		this.error = allowedError;
		this.confidence_relaxation_limit = confidence_relaxation_limit;
		this.secondaryHashSizeFactor = secondaryHashSizeFactor;
		init();
	}
	
	public BJKSTDistinctElements(int numberOfBins,int numMedians,int sizeOfEachMedianSet, int secondaryHashSizeFactor) {
		this.numberOfBins = numberOfBins;
		this.sizeOfMedianSet = sizeOfEachMedianSet;
		this.numMedians = numMedians;
		this.secondaryHashSizeFactor = secondaryHashSizeFactor;
		init();
	}
	
	private void init() {
		int numSecondaryBins = (int)(Math.pow(error,-4.0) * Math.pow(Math.log(numberOfBins), 2));
		for ( int i =0 ; i < numMedians; i++) {
			limits.add(0);
			buffers.add(new HashMap<Integer,Integer>());
			hHashers.add(new Universal2Hasher(numberOfBins,w));
			gHashers.add( new Universal2Hasher((secondaryHashSizeFactor * numSecondaryBins),w));
		}
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		for ( int i =0 ; i < numMedians; i++) {
			String binaryRepr = Integer.toBinaryString(type.getIntegerRepresentation());
			int zereos_p = binaryRepr.length() - binaryRepr.lastIndexOf('1');

		}
		
		
    	_collector.emit(tuple, toPushFurther);
    	_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
}
