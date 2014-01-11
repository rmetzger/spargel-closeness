package de.robertmetzger;

import java.io.IOException;

import eu.stratosphere.api.java.record.io.FileInputFormat;
import eu.stratosphere.types.Record;

/**
 * This input format reads graph data input that has the following format:
 * 
 * <pre>
 * 0	10377,28288,25076,18,26542,11611,26882,10862,26840,2,28965,11456,19,1,11665
 * 1	19,0,18,2,26840
 * 2	19,0,26840,1,18
 * </pre>
 * Which abstracts to
 * {nodeID}{separator}{listOfNodeIDs}
 * 
 * The output of the input format are edges (Long, Long)
 * For the example:
 * <pre>
 * <0,10377>, <0,28288>, ...
 * </pre>
 *
 */
public class VertexInputFormat extends FileInputFormat {
	private static final long serialVersionUID = 1L;

	@Override
	public boolean reachedEnd() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nextRecord(Record record) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
