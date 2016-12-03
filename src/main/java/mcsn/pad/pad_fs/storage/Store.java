package mcsn.pad.pad_fs.storage;

import java.util.Map;

import voldemort.versioning.Versioned;

/**
 * 
 * @author Leonardo Gazzarri
 * 
 * Basic interface that allows put, get and list operations.
 * 
 *
 */
public interface Store<K,V> {
	
	public Versioned<V> get(K key);
	
	public void put(K key, Versioned<V> value);
	
	public Map<K,Versioned<V>> list(Iterable<K> keys);
	
	public void close();

}
