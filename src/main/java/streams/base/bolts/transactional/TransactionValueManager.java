package streams.base.bolts.transactional;

import backtype.storm.tuple.Values;

import java.math.BigInteger;
import java.util.HashMap;

public interface TransactionValueManager {


    Values getLastKnownValue(HashMap objectInfo);

    BigInteger getLastKnownTransactionID(HashMap objectInfo);

    void persistValues(HashMap objectInfo, Values newValue, BigInteger newTransactionID);

}
