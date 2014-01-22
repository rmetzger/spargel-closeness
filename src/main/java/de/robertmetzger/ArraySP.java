package de.robertmetzger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import eu.stratosphere.types.LongValue;

/**
 * Array-based (fixed length shortest path)
 * @author robert
 *
 */
public class ArraySP implements ShortestPath {
	int pathSize;
	private int[] shortestPaths;
	
	public ArraySP(int size) {
		pathSize = size;
		shortestPaths = new int[size];
		Arrays.fill(shortestPaths, -1);
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
		return get(l) != -1;
	}

	@Override
	public int get(int l) {
		return shortestPaths[l];
	}

	@Override
	public void put(int l, int numReachable) {
		shortestPaths[l] = numReachable;
	}

}
