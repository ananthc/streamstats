package streams.base.hashtypes;

public class StringHasherFactory implements BaseHasherFactory {

	
	private String fieldName;
	private int numBins;
	private int wordSize;
	
	public StringHasherFactory (String fieldName,int numBins, int wordSize){
		this.fieldName = fieldName;
		this.numBins = numBins;
		this.wordSize = wordSize;
	}
	
	@Override
	public String2UniversalHasher newHasher() {
		return new String2UniversalHasher(fieldName,numBins,wordSize);
	}

}
