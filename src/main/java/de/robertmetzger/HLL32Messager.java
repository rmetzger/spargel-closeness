package de.robertmetzger;

import eu.stratosphere.spargel.java.MessagingFunction;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.NullValue;

public class HLL32Messager extends MessagingFunction<LongValue, VertexValue, HLL32CounterWritable, NullValue> {
	private static final long serialVersionUID = 1L;

	@Override
	public void sendMessages(LongValue vertexKey, VertexValue vertexValue)
			throws Exception {
		sendMessageToAllNeighbors((HLL32CounterWritable) vertexValue.getCounter());
	}

}
