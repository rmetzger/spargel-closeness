package de.robertmetzger;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.NullValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class LinesToEdges extends MapFunction {

	LongValue fromVal = new LongValue();
	LongValue toVal = new LongValue();
	Record outRec = new Record();
	@Override
	public void map(Record record, Collector<Record> out) throws Exception {
		String line = record.getField(0, StringValue.class).getValue();
		String[] from = line.split("\\t");
		String[] to = from[1].split(",");
		fromVal.setValue( Long.parseLong(from[0]) );
		outRec.setField(0, fromVal);
		for(int i = 0; i < to.length; i++) {
			toVal.setValue(Long.parseLong(to[i]));
			outRec.setField(1, toVal);
			outRec.setField(2, NullValue.getInstance());
			out.collect(outRec);
			// System.err.println("Edge: "+fromVal.getValue()+" -> "+toVal.getValue());
		}
	}
}
