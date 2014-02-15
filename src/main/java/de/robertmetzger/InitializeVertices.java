package de.robertmetzger;

import eu.stratosphere.api.common.accumulators.LongCounter;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class InitializeVertices extends MapFunction {

	LongValue vertexId = new LongValue();
	Record outRec = new Record();
	private LongCounter in;
	private LongCounter outA;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		in = getRuntimeContext().getLongCounter("in lines (vertices)");
		outA = getRuntimeContext().getLongCounter("out tuples (vertices)");
	}
	@Override
	public void map(Record record, Collector<Record> out) throws Exception {
		in.add(1L);
		String[] id = record.getField(0, StringValue.class).getValue().split("\t");
		vertexId.setValue(Long.parseLong(id[0]));
		// initialize with own id:
		
		VertexValue vertexValue = new VertexValue();
		vertexValue.getCounter().addNode(vertexId.getValue()); // not sure if I can initialize it here?
		outRec.setField(0, vertexId);
		outRec.setField(1, vertexValue);
		out.collect(outRec);
		outA.add(1L);
	//	System.err.println("Vertex: "+vertexId.getValue());
	}
}
