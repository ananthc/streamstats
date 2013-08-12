package streams.base.hashtypes;

import backtype.storm.tuple.Tuple;
import streams.base.simplestats.InvalidDataException;


public abstract class BaseHasher {
	

	
	public abstract int hashToInt(Tuple input) throws InvalidDataException;



}
