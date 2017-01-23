package mcsn.pad.pad_fs.storage.local;

import voldemort.versioning.Versioned;

/**
 * 
 * @author Leonardo Gazzarri
 * 
 * Basic interface that allows put, get and list operations.
 * 
 *
 */
public interface IStore<K,V> {
	
	public Versioned<V> get(K key);
	
	public void put(K key, Versioned<V> value);
	
	public void remove(K key);
	
	public Iterable<K> list();
	
	public void close();

}
