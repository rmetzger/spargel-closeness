package de.robertmetzger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Preconditions;

public class HLL32CounterWritable implements Counter {
	
	private static final long serialVersionUID = 1L;
	
	// must be a power of two
	public final static int NUMBER_OF_BUCKETS = 16;
	// may not be larger than 8
	public final static int BITS_PER_BUCKET = 5;

	private final static double ALPHA = 0.709;
	
	private NBitBucketArray buckets;
	
	public HLL32CounterWritable() {
		this.buckets = new NBitBucketArray();
	}

	public void addNode(long n) throws Exception {
		int code = (int)n;
		code = (code + 0x7ed55d16) + (code << 12);
		code = (code ^ 0xc761c23c) ^ (code >>> 19);
		code = (code + 0x165667b1) + (code << 5);
		code = (code + 0xd3a2646c) ^ (code << 9);
		code = (code + 0xfd7046c5) + (code << 3);
		code = (code ^ 0xb55a4f09) ^ (code >>> 16);
		int hash = code >= 0 ? code : -(code + 1);
		
		
		// last 4 bits as bucket index
		int mask = NUMBER_OF_BUCKETS - 1;
		int bucketIndex = hash & mask;
		
		// throw away last 4 bits
		hash >>= 4;
		// make sure the 4 new zeroes don't impact estimate
		hash |= 0xf0000000;
		// hash has now 28 significant bits left
		this.buckets.setBucket(bucketIndex, Integer.numberOfTrailingZeros(hash) + 1);
	}
	
	public long getCount() {
		int count = 0;
		int m2 = NUMBER_OF_BUCKETS*NUMBER_OF_BUCKETS;
		double sum = 0.0;
		for (int i = 0; i < NUMBER_OF_BUCKETS; i++) {
			sum += Math.pow(2.0, -this.buckets.getBucket(i));
		}
		int estimate = (int)(ALPHA*m2*(1.0/sum));
		if(estimate < 2.5*NUMBER_OF_BUCKETS) {
			// look for empty buckets
			int V = 0;
			for (int i = 0; i < NUMBER_OF_BUCKETS; i++) {
				if(buckets.getBucket(i) == 0) {
					V++;
				}
			}
			if(V == 0) {
			    count = estimate;
			} else {
				count = (int)(NUMBER_OF_BUCKETS * Math.log((double)NUMBER_OF_BUCKETS/(double)V));
			}
		} else {
			count = estimate;
		}
		return (long)count;
	}
	
	public void merge(Counter other) throws Exception {
		Preconditions.checkArgument(other instanceof HLL32CounterWritable);
		HLL32CounterWritable oc = (HLL32CounterWritable) other;
		// take the maximum of each bucket pair
		for (int i = 0; i < NUMBER_OF_BUCKETS; i++) {
			if(this.buckets.getBucket(i) < oc.buckets.getBucket(i)) {
				this.buckets.setBucket(i, oc.buckets.getBucket(i));
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(this.buckets.arr);
	}

	@Override
	public void read(DataInput in) throws IOException {
		in.readFully(this.buckets.arr);
	}

}
