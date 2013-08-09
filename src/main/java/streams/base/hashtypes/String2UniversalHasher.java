package streams.base.hashtypes;

import backtype.storm.tuple.Tuple;


public class String2UniversalHasher extends BaseHasher {
    private String feildNameToUse;

    public String2UniversalHasher(String fieldName, int numberOfBins,
        int numOfBitsInWord) {
        super(numberOfBins, numOfBitsInWord);
        this.feildNameToUse = fieldName;
    }

    @Override
    public int getIntegerRepresentation(Tuple input) {
        // TODO Auto-generated method stub
        return 0;
    }
}
