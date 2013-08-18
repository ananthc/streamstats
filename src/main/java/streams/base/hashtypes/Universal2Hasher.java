package streams.base.hashtypes;

import java.io.Serializable;
import java.util.Random;

public class Universal2Hasher implements Serializable {
	
	private int w=32;
	private int m ;
	private int a;
	private int b;
	private int M;
    private Random rand;
	
	
	public Universal2Hasher( int numberOfBins, int numOfBitsInWord) {
		M = getNextNearestIndexPowerOf2(numberOfBins);
		m = (int) Math.pow(2, M);
		w = numOfBitsInWord;
        rand = new Random();
		a = seedAforUniversalHashFamily((int)Math.pow(2, w));
		b=  new Random().nextInt((int)Math.pow(2, (w-m)));
	}
	
	public int seedAforUniversalHashFamily(int max) {
		int finalSeed = 0;
		while (finalSeed % 2 != 1) {
			finalSeed = rand.nextInt(max) ;
		}
		return finalSeed;
	}
	

	
	public int hash(int x) {
		 return ((a*x+b) >>> (w-M));
	}

	

	public static int getNextNearestIndexPowerOf2( int number) {
		return number == 0 ? 0 :(32 - Integer.numberOfLeadingZeros(number - 1));
	}
	



}
