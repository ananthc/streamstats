package streams.base.hashtypes;

public class IntVectorHasherFactory implements BaseHasherFactory {

	
	private String[] fieldNames;
	private int numBins;
	private int wordSize;
	
	public IntVectorHasherFactory(String[] fieldNames, int numBins, int wordSize){
		this.fieldNames = fieldNames;
		this.numBins = numBins;
		this.wordSize = wordSize;
	}
	
	@Override
	public IntVector2UniversalHasher newHasher() {
		return new IntVector2UniversalHasher(fieldNames,numBins,wordSize);
	}

}
