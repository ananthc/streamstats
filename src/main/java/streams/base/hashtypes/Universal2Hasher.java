package streams.base.hashtypes;

import java.util.Random;

public class Universal2Hasher {
	
	private int w=32;
	private int m ;
	private int a;
	private int b;
	private int M;
	
	
	public Universal2Hasher( int numberOfBins, int numOfBitsInWord) {
		M = getNextNearestIndexPowerOf2(numberOfBins);
		m = (int) Math.pow(2, M);
		w = numOfBitsInWord;
		a = seedAforUniversalHashFamily((int)Math.pow(2, w));
		b=  new Random().nextInt((int)Math.pow(2, (w-m)));
	}
	
	public int seedAforUniversalHashFamily(int max) {
		Random rand = new Random();
		int finalSeed = 0;
		while (finalSeed % 2 != 1) {
			finalSeed = rand.nextInt(max) ;
		}
		return finalSeed;
	}
	

	
	public int hash(int x) {
		 return ((a*x+b) >>> (w-M));
	}
	
	private int getPrime(int lowerLimit, int upperLimit) {
        boolean[] isPrime = new boolean[upperLimit + 1];
        for (int i = 2; i <= upperLimit; i++) {
            isPrime[i] = true;
        }
        for (int i = 2; i*i <= upperLimit; i++) {
            if (isPrime[i]) {
                for (int j = i; i*j <= upperLimit; j++) {
                    isPrime[i*j] = false;
                }
            }
        }

        for (int i = lowerLimit; i <= upperLimit; i++) {
            if (isPrime[i]) return i;
        }
		return 0;
	}
	

	public static int getNextNearestIndexPowerOf2( int number) {
		return number == 0 ? 0 :(32 - Integer.numberOfLeadingZeros(number - 1));
	}
	



}
