package de.robertmetzger;

import eu.stratosphere.spargel.java.MessagingFunction;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.NullValue;

// VertexKeyType, VertexValueType, MessageType, EdgeValueType
public class HLLMessager extends MessagingFunction<LongValue, VertexValue, BitfieldCounter, NullValue> {
	private static final long serialVersionUID = 1L;

	@Override
	public void sendMessages(LongValue vertexKey, VertexValue vertexValue)
			throws Exception {
		// System.err.println("Sending message from "+vertexKey+" to all neighbors with count "+vertexValue.getCounter().getCount());
		sendMessageToAllNeighbors((BitfieldCounter) vertexValue.getCounter());
	}

}
