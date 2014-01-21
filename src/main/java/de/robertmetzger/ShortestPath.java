package de.robertmetzger;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.types.LongValue;

public interface ShortestPath extends IOReadableWritable {

	boolean containsKey(int l);

	int get(int l);

	void put(int l, int numReachable);

}
