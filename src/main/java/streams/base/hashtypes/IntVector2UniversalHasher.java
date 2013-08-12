package streams.base.hashtypes;

import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.math.JVMRandom;
import streams.base.simplestats.InvalidConfigException;
import streams.base.simplestats.InvalidDataException;

import java.util.ArrayList;
import java.util.List;

public class IntVector2UniversalHasher extends BaseHasher {

    private String[] feildNamesToUse;

	private int k;

    private int m;

    private int M;

    private int w = 32;

    private List<Long> seedsForVectorHash = new ArrayList<Long>();

    private boolean isKOdd = false;

    private double modValue = Math.pow(2, 64);

    private int divValue = (2 * w )-M;
	
	public IntVector2UniversalHasher(String[] fieldNames, int numBins) {
        k = fieldNames.length;
        M = getNextNearestIndexPowerOf2(numBins);
        m = (int) Math.pow(2, M);
        w = 32;
        divValue = (2 * w )-M;
        modValue = Math.pow(2, (2 * w ));
        isKOdd = (k %2 == 1) ? true : false;
		this.feildNamesToUse = fieldNames;
        for (int i=0 ; i < k; i++) {
            this.seedsForVectorHash.add(this.seedForUniversalHashFamily(Long.MAX_VALUE));
        }
	}

    public long seedForUniversalHashFamily(long max) {
        long finalSeed = 0;
        while (finalSeed % 2 != 1) {
            finalSeed = JVMRandom.nextLong(max) ;
        }
        return finalSeed;
    }

    public static int getNextNearestIndexPowerOf2( int number) {
        return number == 0 ? 0 :(32 - Integer.numberOfLeadingZeros(number - 1));
    }
	
	@Override
	public int hashToInt(Tuple input) throws InvalidDataException {
        for (int k=0; k < feildNamesToUse.length; k++) {
            if (!input.contains(feildNamesToUse[k])) {
                throw new InvalidDataException("The tuple does not contain all of the vector values");
            }
        }

        long sum = 0;
        int firstIndex = 2*i;
        int secondIndex = (2 * i) + 1;
        for (int i=0; i < k/2; i++) {
            firstIndex = 2*i;
            secondIndex = (2 * i) + 1;
            sum = sum + (input.getIntegerByField(this.feildNamesToUse[firstIndex]) + seedsForVectorHash.get(firstIndex)) *
                    (input.getIntegerByField(this.feildNamesToUse[secondIndex])  + seedsForVectorHash.get(secondIndex));
        }
        if ( isKOdd)  {
            sum = sum + (input.getIntegerByField(this.feildNamesToUse[k-1]) + seedsForVectorHash.get(k-1));
        }
        return  (int) (( sum % modValue )  / divValue );
	}

}
