package streams.base.hashtypes;

import backtype.storm.tuple.Tuple;

public class Vector2UniversalHasher extends BaseHasher {

    private String[] feildNamesToUse;
	
	private Universal2Hasher hasher;
	
	public Vector2UniversalHasher(String[] fieldNames,int numberOfBins, int numOfBitsInWord) {
		super(numberOfBins,numOfBitsInWord);
		this.feildNamesToUse = fieldNames;
	}
	
	@Override
	public int getIntegerRepresentation(Tuple input) {
		// TODO Auto-generated method stub
		return 0;
	}

}
