package streams.base.hashtypes;

import backtype.storm.tuple.Tuple;

public abstract class BaseHasher {
	
	private Universal2Hasher hasher;
	
	public BaseHasher(int numberOfBins,int numOfBitsInWord) {
		hasher = new Universal2Hasher(numberOfBins,numOfBitsInWord);
	}
	
	public abstract int getIntegerRepresentation(Tuple input);



}
