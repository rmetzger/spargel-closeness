package de.robertmetzger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.util.SerializableHashMap;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Value;

public class VertexValue implements Value {

	private static final long serialVersionUID = 1L;
	
	private SerializableHashMap<LongValue, LongValue> shortestPath = new SerializableHashMap<LongValue, LongValue>();
	private HLLCounterWritable counter = new HLLCounterWritable();
	
	public SerializableHashMap<LongValue, LongValue> getShortestPath() {
		return this.shortestPath;
	}
	
	public HLLCounterWritable getCounter() {
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
}
