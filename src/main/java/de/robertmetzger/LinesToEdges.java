package de.robertmetzger;

import java.io.Serializable;

import eu.stratosphere.api.common.accumulators.LongCounter;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.NullValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class LinesToEdges extends MapFunction implements Serializable {
	LongValue fromVal = new LongValue();
	LongValue toVal = new LongValue();
	Record outRec = new Record();
	
	private String fromSplit = "\\t";
	private String toSplit = ",";
	private LongCounter in;
	private LongCounter outA;
	public LinesToEdges() {
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		in = getRuntimeContext().getLongCounter("in lines (edges)");
		outA = getRuntimeContext().getLongCounter("out tuples (edges)");
	}
	
	public LinesToEdges(String fromSplit, String toSplit) {
		super();
		this.fromSplit = fromSplit;
		this.toSplit = toSplit;
	}

	@Override
	public void map(Record record, Collector<Record> out) throws Exception {
		in.add(1L);
		String line = record.getField(0, StringValue.class).getValue();
		
		String[] from = line.split(fromSplit);
		fromVal.setValue( Long.parseLong(from[0]) );
		outRec.setField(0, fromVal);
		if(fromSplit.equals(toSplit)) {
			for(int i = 1; i < from.length; i++) {
				toVal.setValue(Long.parseLong(from[i]));
				outRec.setField(1, toVal);
				outRec.setField(2, NullValue.getInstance());
				out.collect(outRec);
				outA.add(1L);
			}
		} else {
			String[] to = from[1].split(toSplit);
			for(int i = 0; i < to.length; i++) {
				toVal.setValue(Long.parseLong(to[i]));
				outRec.setField(1, toVal);
				outRec.setField(2, NullValue.getInstance());
				out.collect(outRec);
				outA.add(1L);
			}
		}
		
	}
}
