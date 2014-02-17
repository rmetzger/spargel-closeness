package de.robertmetzger;



import eu.stratosphere.spargel.java.VertexUpdateFunction;
import eu.stratosphere.spargel.java.util.MessageIterator;
import eu.stratosphere.types.LongValue;



// generic types VertexKeyType, VertexValueType, MessageType
public class HLL32Vertex extends VertexUpdateFunction<LongValue, VertexValue, HLL32CounterWritable> {
	private static final long serialVersionUID = 1L;

	@Override
	public void updateVertex(LongValue vertexKey,
			VertexValue vertexValue, MessageIterator<HLL32CounterWritable> inMessages)
			throws Exception {	
		long seenCountBefore = vertexValue.getCounter().getCount();
		while(inMessages.hasNext()) {
			vertexValue.getCounter().merge(inMessages.next());
		}

		long seenCountAfter = vertexValue.getCounter().getCount();
		
		if(getSuperstep() >= VertexValue.PATHS_SIZE) {
			return;
		}
		if (getSuperstep() > 1) {
			  int l = getSuperstep() - 1;
			  while (l > 0) {
				  if (vertexValue.getShortestPath().containsKey(l)) {
				    break;
				  }
				  l--;
				}
			  
			int numReachable = vertexValue.getShortestPath().get(l);
			
			for (; l < getSuperstep(); l++ ) { 
				vertexValue.getShortestPath().put(l, numReachable);
			}
		}
		vertexValue.getShortestPath().put(getSuperstep(), ((int)vertexValue.getCounter().getCount())-1);
		
		if(seenCountBefore != seenCountAfter) {
			setNewVertexValue(vertexValue);
		}
	}


}
