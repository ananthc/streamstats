package streams.base.hashtypes;

public class IntVectorHasherFactory implements BaseHasherFactory {

	
	private String[] fieldNames;
	private int numBins;

	public IntVectorHasherFactory(String[] fieldNames, int numBins){
		this.fieldNames = fieldNames;
		this.numBins = numBins;
	}
	
	@Override
	public IntVector2UniversalHasher newHasher() {
		return new IntVector2UniversalHasher(fieldNames,numBins);
	}

    @Override
    public int getNumBins() {
        return numBins;
    }

    @Override
    public void setNumBins(int numBins) {
        this.numBins = numBins;
    }

}
