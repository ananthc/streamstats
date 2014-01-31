package streams.base.hashtypes;


import streams.base.simplestats.InvalidConfigException;

import java.io.Serializable;

public class CompositeHasherFactory  implements   BaseHasherFactory, Serializable {

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

    @Override
    public int getNumBins() {
        return numBins;
    }

    @Override
    public void setNumBins(int numBins) {
        this.numBins = numBins;
    }

    public int getMaxStringLength() {
        return maxStringLength;
    }

    public void setMaxStringLength(int maxStringLength) {
        this.maxStringLength = maxStringLength;
    }
}
