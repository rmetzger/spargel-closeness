package de.robertmetzger;


import eu.stratosphere.types.Value;

public interface Counter extends Value {
	public void merge(Counter other) throws Exception;
	public long getCount();
	public void addNode(long value) throws Exception;
}
