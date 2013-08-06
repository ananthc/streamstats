package streams.base.hashtypes;

import backtype.storm.tuple.Tuple;

public class Integer2UniversalHasher extends BaseHasher {

	private String feildNameToUse;
	
	
	
	public Integer2UniversalHasher(String fieldName,int numberOfBins, int numOfBitsInWord) {
		super(numberOfBins,numOfBitsInWord);
		this.feildNameToUse = fieldName;
		
	}
	
	@Override
	public int getIntegerRepresentation(Tuple input) {
		// TODO Auto-generated method stub
		return 0;
	}

}
