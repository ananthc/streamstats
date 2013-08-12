package streams.base.hashtypes;

import backtype.storm.tuple.Tuple;
import streams.base.simplestats.InvalidDataException;

public class Integer2UniversalHasher extends BaseHasher {

	private String fieldNameToUse;

    private Universal2Hasher hasher;

	
	public Integer2UniversalHasher(String fieldName,int numberOfBins, int numOfBitsInWord) {
        this.hasher = new Universal2Hasher(numberOfBins,numOfBitsInWord);
		this.fieldNameToUse = fieldName;
		
	}
	
	@Override
	public int hashToInt(Tuple input) throws InvalidDataException  {
        if (!input.contains(fieldNameToUse)) {
            throw new InvalidDataException("The tuple did not contain that value");
        }
        int valueToHash = input.getIntegerByField(fieldNameToUse);
        return hasher.hash(valueToHash);
	}

}
