package de.robertmetzger;

import java.util.Iterator;

import eu.stratosphere.spargel.java.VertexUpdateFunction;
import eu.stratosphere.types.LongValue;



// generic types VertexKeyType, VertexValueType, MessageType
public class HLLVertex extends VertexUpdateFunction<LongValue, VertexValue, Counter> {
	private static final long serialVersionUID = 1L;

	@Override
	public void updateVertex(LongValue vertexKey,
			VertexValue vertexValue, Iterator<Counter> inMessages)
			throws Exception {
	//	System.err.println("Updating vertext "+vertexKey.getValue()+" on superstep "+getSuperstep());
		
		
		long seenCountBefore = vertexValue.getCounter().getCount();
	//	System.err.println("seenCountBefore="+seenCountBefore);
		while(inMessages.hasNext()) {
		//	System.err.println("merging with message ");
			vertexValue.getCounter().merge(inMessages.next());
		}

		long seenCountAfter = vertexValue.getCounter().getCount();
		
	//	System.err.println("seenCountAfter="+seenCountAfter);
		

		//if ((seenCountBefore != seenCountAfter) || (getSuperstep() == 1)) {
		//	System.err.println("Writing. SP size "+vertexValue.getShortestPath().size());
			
		// }

		// determine last iteration for which we set a value,
		// we need to copy this to all iterations up to this one
		// because the number of reachable vertices stays the same
		// when the compute method is not invoked
		if (getSuperstep() > 1) {
			  int l = getSuperstep() - 1;
			  while (l > 0) {
				  if (vertexValue.getShortestPath().containsKey(l)) {
				    break;
				  }
				  l = l-1;
				}
			  
			int numReachable = vertexValue.getShortestPath().get(l);
			
			for (; l < getSuperstep(); l++ ) { 
			//	System.err.println("Adding values to shortest path");
				vertexValue.getShortestPath().put(l, numReachable);
			}
		}
		// subtract 1 because our own bit is counted as well
	//	System.err.println("Putting <"+getSuperstep()+";"+(vertexValue.getCounter().getCount()-1L)+"> for vertex "+vertexKey.getValue());
		vertexValue.getShortestPath().put(getSuperstep(), ((int)vertexValue.getCounter().getCount())-1);
		
//		System.err.println("+++ Debugging n nodes reachable within x steps (x,n) for "+vertexKey.getValue());
//		ShortestPath hm = vertexValue.getShortestPath();
//		for( Entry<LongValue, LongValue> e: hm.entrySet()) {
//			System.err.println("+++ "+e.getKey().getValue()+";"+e.getValue().getValue());
//		}
		if(seenCountBefore != seenCountAfter) {
			setNewVertexValue(vertexValue);
		}
	}


}
