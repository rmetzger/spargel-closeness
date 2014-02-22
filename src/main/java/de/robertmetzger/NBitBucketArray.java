package de.robertmetzger;

import com.google.common.base.Preconditions;

public class NBitBucketArray {

	byte[] arr;
	int bitsPerBucket;
	int numberOfBuckets;
	
	public NBitBucketArray(int bitsPerBucket, int numberOfBuckets) throws Exception{
		if(bitsPerBucket > Byte.SIZE) {
			throw new Exception("Buckets larger than " + Byte.SIZE + " bits not supported.");
		}
		this.bitsPerBucket = bitsPerBucket;
		this.numberOfBuckets = numberOfBuckets;
		int arrSize = (int) Math.ceil((double)(bitsPerBucket*numberOfBuckets)/(double)Byte.SIZE);
		this.arr = new byte[arrSize];
	}
	
	public byte[] getBytes() {
		return arr;
	}
	
	public void setBytes(byte[] arr) {
		Preconditions.checkArgument(this.arr.length == arr.length);
		this.arr = arr;
	}
	
	public void setBucket(int bucketIndex, int val) throws Exception{
		if((double)val > (Math.pow(2.0, (double) bitsPerBucket)) - 1.0) {
			throw new Exception("Value too large to fit array.");
		}
		int bitIndex = bucketIndex*this.bitsPerBucket;
		int firstBucket = bitIndex/Byte.SIZE;
		int lastBucket = (bitIndex + bitsPerBucket - 1)/Byte.SIZE;
		int firstIndex = bitIndex % Byte.SIZE;
		byte value = (byte) val;
		byte mask = (byte) (0x01 << (bitsPerBucket-1));
		int index = 0;
		while(index < this.bitsPerBucket) {
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
		int bitIndex = bucketIndex*this.bitsPerBucket;
		int firstBucket = bitIndex/Byte.SIZE;
		int lastBucket = (bitIndex + bitsPerBucket - 1)/Byte.SIZE;
		int firstIndex = bitIndex % Byte.SIZE;
		byte mask = (byte)(1 << (bitsPerBucket-1));
		byte value = 0x00;
		int index = 0;
		while(index < this.bitsPerBucket) {
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
		
	}
}
