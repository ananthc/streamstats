package streams.base.hashtypes;

public class StringHasherFactory implements BaseHasherFactory {

	
	private String fieldName;
	private int numBins;
	private int maxStringLength;
	
	public StringHasherFactory (String fieldName,int numBins, int maxStringLength){
		this.fieldName = fieldName;
		this.numBins = numBins;
		this.maxStringLength = maxStringLength;
	}
	
	@Override
	public String2UniversalHasher newHasher() {
		return new String2UniversalHasher(fieldName,numBins,maxStringLength);
	}

}
