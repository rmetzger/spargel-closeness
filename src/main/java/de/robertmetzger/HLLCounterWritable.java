package de.robertmetzger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Preconditions;

import eu.stratosphere.types.Value;

/**
 * A counter for counting unique vertex IDs using a HyperLogLog sketch.
 * 
 */
public class HLLCounterWritable implements Counter {
	private static final long serialVersionUID = 1L;
	
	// must be a power of two
	public final static int NUMBER_OF_BUCKETS = 64;
	private final static double ALPHA = 0.709;
	
	private byte[] buckets;
	
	public HLLCounterWritable() {
		this.buckets = new byte[NUMBER_OF_BUCKETS];
		
	}
	
	public void addNode(long n) {
		/* Murmur 2.0 hash START */
		long m = 0xc6a4a7935bd1e995L;
		int r = 47;
		      
		byte[] data = new byte[8];
		for (int i = 0; i < data.length; i++) {
		  data[i] = (byte) n;
		  n >>= 8;
		}

		long hash = (1L & 0xffffffffL) ^ (8 * m);
		long k =  ( (long) data[0] & 0xff)        + (((long) data[1] & 0xff) << 8)
		        + (((long) data[2] & 0xff) << 16) + (((long) data[3] & 0xff) << 24)
		        + (((long) data[4] & 0xff) << 32) + (((long) data[5] & 0xff) << 40)
		        + (((long) data[6] & 0xff) << 48) + (((long) data[7] & 0xff) << 56);
		            
		k *= m;
		k ^= k >>> r;
		k *= m;
		            
		hash ^= k;
		hash *= m;
		hash *= m;
		hash ^= hash >>> r;
		hash *= m;
		hash ^= hash >>> r;
		/* Murmur 2.0 hash END */
		
		// last 6 bits as bucket index
		int mask = NUMBER_OF_BUCKETS - 1;
		int bucketIndex = (int) (hash & mask);
		
		// throw away last 6 bits
		hash >>= 6;
		// make sure the 6 new zeroes don't impact estimate
		hash |= 0xfc00000000000000L;
		// hash has now 58 significant bits left
		this.buckets[bucketIndex] = (byte) (Long.numberOfTrailingZeros(hash) + 1L);
	}
	
	public long getCount() {
		long count = 0L;
		int m2 = NUMBER_OF_BUCKETS*NUMBER_OF_BUCKETS;
		double sum = 0.0;
		for (int i = 0; i < buckets.length; i++) {
			sum += Math.pow(2.0, -buckets[i]);
		}
		long estimate = (long)(ALPHA*m2*(1.0/sum));
		if(estimate < 2.5*NUMBER_OF_BUCKETS) {
			// look for empty buckets
			int V = 0;
			for (int i = 0; i < buckets.length; i++) {
				if(buckets[i] == 0) {
					V++;
				}
			}
			if(V == 0) {
			    count = estimate;
			} else {
				count = (long) (NUMBER_OF_BUCKETS * Math.log((double)NUMBER_OF_BUCKETS/(double)V));
			}
		} else {
			count = estimate;
		}
		return count;
	}
	
	public void merge(Counter other) {
		Preconditions.checkArgument(other instanceof HLLCounterWritable);
		HLLCounterWritable oc = (HLLCounterWritable) other;
		// take the maximum of each bucket pair
		for (int i = 0; i < this.buckets.length; i++) {
			if(this.buckets[i] < oc.buckets[i]) {
				this.buckets[i] = oc.buckets[i];
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(this.buckets);
	}

	@Override
	public void read(DataInput in) throws IOException {
		in.readFully(this.buckets);
	}

}
