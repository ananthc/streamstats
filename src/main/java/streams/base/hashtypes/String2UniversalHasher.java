package streams.base.hashtypes;

import backtype.storm.tuple.Tuple;
import streams.base.simplestats.InvalidConfigException;
import streams.base.simplestats.InvalidDataException;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;



public class String2UniversalHasher extends BaseHasher {

    private String fieldNameToUse;

    private int m;

    private int M;

    private int w;

    private Random rand;

    private List<Integer> primes = new ArrayList<Integer>();

    private int lowestCharNumber = 2048;

    private int maxStringLength = 256;

    private int lowerLimitForPrimes = maxStringLength;

    private Universal2Hasher hasher;

    public String2UniversalHasher(String fieldName, int numberOfBins, int maxStringLength) throws InvalidConfigException {
        M = getNextNearestIndexPowerOf2(numberOfBins);
        m = (int) Math.pow(2, M);
        w = 32;
        this.maxStringLength =  maxStringLength;
        if (maxStringLength < lowestCharNumber) {
            lowerLimitForPrimes = lowestCharNumber;
        }
        else {
            lowerLimitForPrimes = maxStringLength;
        }
        rand = new Random();
        hasher = new Universal2Hasher(m,w);
        this.fieldNameToUse = fieldName;
        if ( m > lowerLimitForPrimes)  {
            lowerLimitForPrimes = m;
        }
        primes = this.getPrimes(lowerLimitForPrimes,Integer.MAX_VALUE,(maxStringLength + 1) );
    }


    private List<Integer> getPrimes(int lowerLimit, int upperLimit,int count) throws InvalidConfigException {
        List<Integer> returnList = new ArrayList<Integer>(count);
        boolean[] isPrime = new boolean[upperLimit + 1];
        for (int i = 2; i <= upperLimit; i++) {
            isPrime[i] = true;
        }
        for (int i = 2; i*i <= upperLimit; i++) {
            if (isPrime[i]) {
                for (int j = i; i*j <= upperLimit; j++) {
                    isPrime[i*j] = false;
                }
            }
        }
        Random rand = new Random();
        int difference = upperLimit - lowerLimit;
        if (difference < 0) {
            throw new InvalidConfigException("Illegal lower and upper limits given for prime ranges");
        }
        int k = 0;
        while ( k < count) {
            int nextStartingPoint = rand.nextInt(difference);
            for (int j = (lowerLimit + nextStartingPoint); j <= upperLimit; j++) {
                if (isPrime[j]) {
                    returnList.add(j);
                    k++;
                }
            }
        }
        return returnList;
    }


    public static int getNextNearestIndexPowerOf2( int number) {
        return number == 0 ? 0 :(32 - Integer.numberOfLeadingZeros(number - 1));
    }

    @Override
    public int hashToInt(Tuple input) throws InvalidDataException {
        if (!input.contains(fieldNameToUse)) {
            throw new InvalidDataException("The tuple does not contain the value");
        }
        String currentValue = input.getStringByField(fieldNameToUse);
        if (currentValue == null)
            throw new InvalidDataException("The tuple contains a null value");
        int maxLength = maxStringLength;
        int currentStringLength = currentValue.length();
        if (currentStringLength < maxStringLength) {
            int difference =  (maxStringLength -  currentStringLength);
            currentValue = currentValue + String.format(("%0"+difference+"d"), 0);
        }
        if (currentStringLength > maxStringLength) {
            currentValue = currentValue.substring(0,maxStringLength);
        }
        int sum = 0;
        for (int i=0 ; i < maxLength; i++) {
            sum = sum + (((int) currentValue.charAt(i)) * (primes.get(i)) );
        }
        return hasher.hash( (sum % primes.get(maxStringLength)));
    }
}
