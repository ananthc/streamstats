package streams.base.hashtypes;

import streams.base.simplestats.InvalidConfigException;

import java.io.Serializable;

public class StringHasherFactory implements BaseHasherFactory, Serializable {

	
	private String fieldName;
	private int numBins;
	private int maxStringLength;
	
	public StringHasherFactory (String fieldName,int numBins, int maxStringLength){
		this.fieldName = fieldName;
		this.numBins = numBins;
		this.maxStringLength = maxStringLength;
	}
	
	@Override
	public String2UniversalHasher newHasher() throws InvalidConfigException {
		return new String2UniversalHasher(fieldName,numBins,maxStringLength);
	}

}
