package streams.base.hashtypes;


import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.math.JVMRandom;
import streams.base.simplestats.InvalidConfigException;
import streams.base.simplestats.InvalidDataException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Composite2UniversalHasher extends BaseHasher implements Serializable {

    private String[] fieldNames;

    private CompositeHashDataType[] types;

    private int numBins;

    private int maxStringLength;

    private List<BaseHasher> hashers = new ArrayList<BaseHasher>();

    private List<Long> seedsForVectorHash = new ArrayList<Long>();

    private int k;

    private int m;

    private int M;

    private int w = 32;

    private boolean isKOdd = false;

    private double modValue = Math.pow(2, 64);

    private int divValue = (2 * w )-M;


    public Composite2UniversalHasher(String[] fieldNames,CompositeHashDataType[] types, int numBins, int maxStringLength) throws InvalidConfigException {
        this.fieldNames = fieldNames;
        this.types = types;
        this.numBins = numBins;
        this.maxStringLength = maxStringLength;
        k = fieldNames.length;
        M = getNextNearestIndexPowerOf2(numBins);
        m = (int) Math.pow(2, M);
        w = 32;
        divValue = (2 * w )-M;
        modValue = Math.pow(2, (2 * w ));
        isKOdd = (k %2 == 1) ? true : false;

        for (int i=0 ; i < fieldNames.length; i++) {
            switch (types[i]) {
                case INTEGER:
                    hashers.add(new Integer2UniversalHasher(fieldNames[i],numBins,w));
                    break;
                case STRING:
                    hashers.add(new String2UniversalHasher(fieldNames[i],numBins,maxStringLength));
                    break;
            }
        }
        for (int i=0 ; i < fieldNames.length; i++) {
            this.seedsForVectorHash.add(this.seedForUniversalHashFamily(Long.MAX_VALUE));
        }
    }
    @Override
    public int hashToInt(Tuple input) throws InvalidDataException {
        for (int j=0; j < fieldNames.length; j++) {
            if (!input.contains(fieldNames[j])) {
                throw new InvalidDataException("The tuple does not contain all of the vector values");
            }
        }
        long sum = 0;
        int firstIndex = 0;
        int secondIndex =  1;
        for (int i=0; i < k/2; i++) {
            firstIndex = 2*i;
            secondIndex = (2 * i) + 1;
            sum = sum + ( hashers.get(firstIndex).hashToInt(input) + seedsForVectorHash.get(firstIndex)) *
                    (hashers.get(secondIndex).hashToInt(input)  + seedsForVectorHash.get(secondIndex));
        }
        if (isKOdd)  {
            sum = sum + (hashers.get(k-1).hashToInt(input)  + seedsForVectorHash.get(k-1));
        }
        return  (int) (( sum % modValue )  / divValue );

    }

    public static int getNextNearestIndexPowerOf2( int number) {
        return number == 0 ? 0 :(32 - Integer.numberOfLeadingZeros(number - 1));
    }

    public long seedForUniversalHashFamily(long max) {
        long finalSeed = 0;
        while (finalSeed % 2 != 1) {
            finalSeed = JVMRandom.nextLong(max) ;
        }
        return finalSeed;
    }



}
