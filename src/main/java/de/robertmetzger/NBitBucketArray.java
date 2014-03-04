package de.robertmetzger;

public class NBitBucketArray {

	byte[] arr;
	
	public NBitBucketArray() {
		int arrSize = (int) Math.ceil((double)(HLL32CounterWritable.BITS_PER_BUCKET*HLL32CounterWritable.NUMBER_OF_BUCKETS)/(double)Byte.SIZE);
		this.arr = new byte[arrSize];
	}
	
	public void setBucket(int bucketIndex, int val) {
		//if((double)val > (Math.pow(2.0, (double) HLL32CounterWritable.BITS_PER_BUCKET)) - 1.0) {
		//	throw new Exception("Value too large to fit array.");
		//}
		int bitIndex = bucketIndex*HLL32CounterWritable.BITS_PER_BUCKET;
		int firstBucket = bitIndex/Byte.SIZE;
		int lastBucket = (bitIndex + HLL32CounterWritable.BITS_PER_BUCKET - 1)/Byte.SIZE;
		int firstIndex = bitIndex % Byte.SIZE;
		byte value = (byte) val;
		byte mask = (byte) (0x01 << (HLL32CounterWritable.BITS_PER_BUCKET-1));
		int index = 0;
		while(index < HLL32CounterWritable.BITS_PER_BUCKET) {
			byte tmp = arr[firstBucket];
			if((value & mask) != 0) { // 1 at nth bit
				arr[firstBucket] = (byte) (tmp | (0x01 << firstIndex));
			} else { // 0 at nth bit
				arr[firstBucket] = (byte) (tmp & (~(0x01 << firstIndex)));
			}
			firstIndex++;
			firstIndex = firstIndex % Byte.SIZE;
			if(firstIndex == 0) {
				firstBucket = lastBucket;
			}
			mask >>= 1;
			index++;
		}
	}
	
	public int getBucket(int bucketIndex) {
		int bitIndex = bucketIndex*HLL32CounterWritable.BITS_PER_BUCKET;
		int firstBucket = bitIndex/Byte.SIZE;
		int lastBucket = (bitIndex + HLL32CounterWritable.BITS_PER_BUCKET - 1)/Byte.SIZE;
		int firstIndex = bitIndex % Byte.SIZE;
		byte mask = (byte)(1 << (HLL32CounterWritable.BITS_PER_BUCKET-1));
		byte value = 0x00;
		int index = 0;
		while(index < HLL32CounterWritable.BITS_PER_BUCKET) {
			byte tmp = arr[firstBucket];
			if((tmp & (1 << firstIndex)) != 0) {
				value |= mask;
			}
			firstIndex = ++firstIndex % Byte.SIZE;
			if(firstIndex == 0) {
				firstBucket = lastBucket;
			}
			mask >>= 1;
			index++;
		}
		return (int) value;
	}

	public void mergeBuckets(NBitBucketArray other) {
		/*
		int size = ((HLL32CounterWritable.NUMBER_OF_BUCKETS*HLL32CounterWritable.BITS_PER_BUCKET) + Long.SIZE - 1)/Long.SIZE; // round up
		long[] arr1 = new long[size];
		long[] arr2 = new long[size];
		for (int i = 0; i < arr1.length; i++) {
			for (int j = 0; j < this.arr.length; j++) {
				arr1[i] += ((long) this.arr[j] & 0xffL) << (Byte.SIZE * j);
				arr2[i] += ((long) other.arr[j] & 0xffL) << (Byte.SIZE * j);
			}
		}
		for (int i = 0; i < arr1.length; i++) {
			
		}
		for (int i = 0; i < arr1.length; i++) {
			for (int j = 0; j < this.arr.length; j++) {
				this.arr[(i * (Long.SIZE/Byte.SIZE)) + j] = (byte) (arr1[i] >> (Byte.SIZE * j));
			}
		}
		*/
	}
}
