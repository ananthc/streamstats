package streams.base.simplestats;


import backtype.storm.tuple.Tuple;

public interface CountSpecifier {

    int getCount(Tuple input);

}
