package streams.base.hashtypes;

import java.io.Serializable;

public class IntegerHasherFactory implements BaseHasherFactory, Serializable {

	private String fieldName;
	private int numBins;
	private int wordSize;
	
	public IntegerHasherFactory(String fieldName,int numBins, int wordSize) {
		this.fieldName = fieldName;
		this.numBins = numBins;
		this.wordSize = wordSize;
	}
	
	@Override
	public Integer2UniversalHasher newHasher() {
		return new Integer2UniversalHasher(fieldName,numBins,wordSize);
	}



}
