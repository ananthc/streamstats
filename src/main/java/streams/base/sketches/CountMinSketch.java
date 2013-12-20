package streams.base.sketches;


import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import streams.base.hashtypes.BaseHasher;
import streams.base.hashtypes.BaseHasherFactory;
import streams.base.simplestats.InvalidDataException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/*
 http://dimacs.rutgers.edu/~graham/pubs/papers/cmencyc.pdf
 http://dimacs.rutgers.edu/~graham/pubs/papers/cm-latin.pdf

*/

public class CountMinSketch<T extends BaseHasherFactory> implements Serializable{

    protected float errorFactor = 0.02f;

    protected float probabilityOfErrror = 0.1f;

    protected streams.base.simplestats.CountSpecifier countSpecifier = null;

    protected  int numBins = 20;

    protected Integer[][] sketchArray = null;

    private List<BaseHasher> hashers = new ArrayList<BaseHasher>();

    private int numRows;

    private int numCols;


    public CountMinSketch(T hasherFactory, float errorFactor, float probabilityOfErrror,
                          streams.base.simplestats.CountSpecifier countSpecifier) throws streams.base.simplestats.InvalidConfigException {

        this.errorFactor = errorFactor;
        this.probabilityOfErrror = probabilityOfErrror;
        this.countSpecifier = countSpecifier;
        init(hasherFactory);
    }

    public CountMinSketch(T hasherFactory,float errorFactor, float probabilityOfErrror) throws streams.base.simplestats.InvalidConfigException {
        this.errorFactor = errorFactor;
        this.probabilityOfErrror = probabilityOfErrror;
        this.countSpecifier = new streams.base.simplestats.CountSpecifier() {
            @Override
            public int getCount(Tuple input) {
                return 1;
            }
        };
        init(hasherFactory);
    }

    private void init(T hasherFactory) throws streams.base.simplestats.InvalidConfigException {
        numCols = (int) Math.ceil( (2) / this.errorFactor);
        numRows = (int) Math.ceil(Math.log( (1) / this.probabilityOfErrror));
        for (int i = 0; i < numRows; i++) {
            hashers.add(hasherFactory.newHasher());
        }
        sketchArray = new Integer[numRows][numCols];
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                sketchArray[i][j] = 0;
            }
        }
    }


    public Values processTuple(Tuple input) throws InvalidDataException {
        int columnIndex = 0;
        int lowestRowVal = Integer.MIN_VALUE;
        for (int i = 0; i < numRows; i++) {
            columnIndex = hashers.get(i).hashToInt(input);
            sketchArray[i][columnIndex] = sketchArray[i][columnIndex] + countSpecifier.getCount(input);
            if (lowestRowVal > sketchArray[i][columnIndex] )
                lowestRowVal = sketchArray[i][columnIndex];
        }
        Values returnValue = new Values();
        returnValue.add(0,lowestRowVal);
        return returnValue;
    }


}



