package streams.base.hashtypes;


import streams.base.simplestats.InvalidConfigException;

public class CompositeHasherFactory  implements   BaseHasherFactory {

    private String[] fieldNames;

    private CompositeHashDataType[] types;

    private int numBins;

    private int maxStringLength;

    public CompositeHasherFactory(String[] fieldNames,CompositeHashDataType[] types, int numBins, int maxStringLength) {
        this.fieldNames = fieldNames;
        this.types = types;
        this.numBins = numBins;
        this.maxStringLength = maxStringLength;
    }

    @Override
    public BaseHasher newHasher() throws InvalidConfigException {
        return new Composite2UniversalHasher(fieldNames, types,  numBins,  maxStringLength);
    }
}
