package streams.base.hashtypes;

public class VectorHasherFactory implements BaseHasherFactory {

	
	private String[] fieldNames;
	private int numBins;
	private int wordSize;
	
	public VectorHasherFactory (String[] fieldNames,int numBins, int wordSize){
		this.fieldNames = fieldNames;
		this.numBins = numBins;
		this.wordSize = wordSize;
	}
	
	@Override
	public Vector2UniversalHasher newHasher() {
		return new Vector2UniversalHasher(fieldNames,numBins,wordSize);
	}

}
