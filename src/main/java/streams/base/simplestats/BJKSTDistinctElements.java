package streams.base.simplestats;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import streams.base.hashtypes.BaseHasher;
import streams.base.hashtypes.BaseHasherFactory;

import java.io.Serializable;
import java.util.*;

public class BJKSTDistinctElements<T extends BaseHasherFactory>  implements Serializable {
	

	private static final long serialVersionUID = -2032575802259420762L;
	
	private int numberOfBins = 32;
	
	private int w = 32;
	
	private int numMedians=25;
	
	private float error = 0.02f;
	
	private float confidence_relaxation_limit = 0.05f;
	
	private int sizeOfMedianSet;

	T primaryHasherFactory;
	
	T seondaryHasherFactory;
	
	
	// make this large enough as possible. This one effects the permissible size of the buffer bins for the secondary hash function
	private int secondaryHashSizeFactor ;
	
	
	// make c as large as possible. This reflects the size of the buffer 
	private int c;

	
	private List<Integer> limits = new ArrayList<Integer>();
	
    private int bufferSize = 100;

	private List<HashSet<String>> buffers = new ArrayList<HashSet<String>>();
	
	private List<BaseHasher> hHashers = new ArrayList<BaseHasher>();
	
	private List<BaseHasher> gHashers = new ArrayList<BaseHasher>();

    private int intLength = Integer.toString(Integer.MAX_VALUE).length();

    private String  lengthOfIntegerRepresentation = null;

	public BJKSTDistinctElements(T firstHasher , T seondaryHasher ,int numberOfMedianAttempts , 
												int sizeOfEachMedianSet, int secondaryHashSizeFactor )
														throws InvalidConfigException  {
		this.primaryHasherFactory = firstHasher;
		this.seondaryHasherFactory = seondaryHasher;
		this.numMedians = numberOfMedianAttempts;
		this.sizeOfMedianSet = sizeOfEachMedianSet;
		this.secondaryHashSizeFactor = secondaryHashSizeFactor;
		init();
	}

	public BJKSTDistinctElements(T firstHasher , T seondaryHasher ,int numberOfMedianAttempts , int sizeOfEachMedianSet, 
			                                       float allowedError, float confidence_relaxation_limit,int secondaryHashSizeFactor )
	               throws InvalidConfigException {
		if (allowedError > 1) {
			throw new InvalidConfigException("Permitted error should be < 1 and in float format");
		}
		if (confidence_relaxation_limit > 1) {
			throw new InvalidConfigException("Permitted confidence_relaxation_limit should be < 1 and in float format");
		}
		this.primaryHasherFactory = firstHasher;
		this.seondaryHasherFactory = seondaryHasher;
		this.numMedians = numberOfMedianAttempts;
		this.sizeOfMedianSet = sizeOfEachMedianSet;
		this.error = allowedError;
		this.confidence_relaxation_limit = confidence_relaxation_limit;
		this.secondaryHashSizeFactor = secondaryHashSizeFactor;
		init();
	}
	
	public static int getNumberOfSecondaryBinsForPrimaryBins(int primaryBinsNumber,float errorTolerance) {
		return (int)(Math.pow(errorTolerance,-4.0) * Math.pow(Math.log(primaryBinsNumber), 2));
	}
	
	private void init() throws InvalidConfigException {
        this.bufferSize =  (int) ((this.sizeOfMedianSet) / Math.pow(this.error,2.0) ) ;
        lengthOfIntegerRepresentation = ("%0" + intLength + "d");
		for ( int i =0 ; i < numMedians; i++) {
			limits.add(0);
  			buffers.add(new HashSet<String>());
			hHashers.add(primaryHasherFactory.newHasher());
			gHashers.add(seondaryHasherFactory.newHasher());
		}
	}
	


	public Values processTuple(Tuple tuple) throws InvalidDataException {
		for ( int i =0 ; i < numMedians; i++) {
            String binaryRepr = null;
            try {
                binaryRepr = Integer.toBinaryString(hHashers.get(i).hashToInt(tuple));
            } catch (InvalidDataException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            int zereosP = binaryRepr.length() - binaryRepr.lastIndexOf('1');
            int currentZ = limits.get(i);
            if (zereosP >= currentZ) {
                HashSet<String> currentBuffer = buffers.get(i);
                try {
                    currentBuffer.add(String.format(lengthOfIntegerRepresentation, gHashers.get(i).hashToInt(tuple)) +
                            String.format(lengthOfIntegerRepresentation, zereosP));
                } catch (InvalidDataException e) {
                    e.printStackTrace();
                }
                while (currentBuffer.size() > bufferSize    ) {
                    currentZ = currentZ + 1;
                    for (Iterator<String> itr = currentBuffer.iterator(); itr.hasNext();) {
                        String element = itr.next();
                        int zeroesOld = Integer.parseInt(element.substring(intLength));
                        if (zeroesOld < currentZ) {
                            itr.remove();
                        }
                    }
                }
            }
		}
		HashMap<Integer,Integer> results = new HashMap<Integer,Integer>();
        for ( int i =0 ; i < numMedians; i++) {
            int currentGuess = (int)  (buffers.get(i).size() * Math.pow(2,limits.get(i)));
            if (!results.containsKey(currentGuess)) {
                results.put(currentGuess,1);
            }
            else {
                int currentCount = results.get(currentGuess);
                results.put(currentGuess,(currentCount + 1));
            }
        }
        int finalEstimate = 0;
        int highestVote = 0;
        Iterator it = results.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer,Integer> pair = (Map.Entry<Integer,Integer>)it.next();
            int possibleAnswer = pair.getValue();
            if (possibleAnswer > highestVote ) {
                highestVote = possibleAnswer;
                finalEstimate = pair.getKey();
            }
        }
        return  new Values(finalEstimate);
	}

}
