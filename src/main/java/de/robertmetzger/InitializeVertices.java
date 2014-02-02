package de.robertmetzger;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class InitializeVertices extends MapFunction {

	LongValue vertexId = new LongValue();
	VertexValue vertexValue = new VertexValue();
	Record outRec = new Record();
	
	@Override
	public void map(Record record, Collector<Record> out) throws Exception {
		String[] id = record.getField(0, StringValue.class).getValue().split("\t");
		vertexId.setValue(Long.parseLong(id[0]));
		// initialize with own id:
		vertexValue.getCounter().addNode(vertexId.getValue()); // not sure if I can initialize it here?
		outRec.setField(0, vertexId);
		outRec.setField(1, vertexValue);
		out.collect(outRec);
	//	System.err.println("Vertex: "+vertexId.getValue());
	}
}
