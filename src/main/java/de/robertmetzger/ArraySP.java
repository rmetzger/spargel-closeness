package de.robertmetzger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.types.LongValue;

public class ArraySP implements ShortestPath {
	int pathSize;
	private int[] shortestPaths;
	
	public ArraySP(int size) {
		pathSize = size;
		shortestPaths = new int[size];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for(int i = 0; i < pathSize; i++) {
			out.writeInt(shortestPaths[i]);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		for(int i = 0; i < pathSize; i++) {
			shortestPaths[i] = in.readInt();
		}
	}

	@Override
	public boolean containsKey(int l) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int get(int l) {
		return shortestPaths[l];
	}

	@Override
	public void put(int l, int numReachable) {
		// TODO Auto-generated method stub
		
	}

}
