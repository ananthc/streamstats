package streams.base.hashtypes;

import streams.base.simplestats.InvalidConfigException;

public interface BaseHasherFactory {
	
	
	BaseHasher newHasher() throws InvalidConfigException;

	

}
