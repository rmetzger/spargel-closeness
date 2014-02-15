package de.robertmetzger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.util.SerializableHashMap;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Value;

public class VertexValue implements Value {

	final public static int PATHS_SIZE = 30;
	
	
	private static final long serialVersionUID = 1L;
	
	private ShortestPath shortestPath = new ArraySP(PATHS_SIZE);
	// private SerializableHashMap<LongValue, LongValue> shortestPath = new SerializableHashMap<LongValue, LongValue>();
	
	private Counter counter = new HLLCounterWritable();
	
	public ShortestPath getShortestPath() {
		return this.shortestPath;
	}
	
	public Counter getCounter() {
		return this.counter;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		counter.write(out);
		shortestPath.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		counter.read(in);
		shortestPath.read(in);
	}
	
	/**
	 * Output to file.
	 */
	@Override
	public String toString() {
		return this.shortestPath.stringify();
	}
}
