package streams.base.simplestats;


import backtype.storm.tuple.Tuple;

import java.io.Serializable;

public interface CountSpecifier extends Serializable {

    int getCount(Tuple input);

}
