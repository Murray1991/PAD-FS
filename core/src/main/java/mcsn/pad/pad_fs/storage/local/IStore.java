package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import java.util.List;

import voldemort.versioning.VectorClock;
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
	
	public List<Versioned<V>> get(K key);
	
	public void put(K key, Versioned<V> value);
	
	public void put(List<Serializable> keys, List<Versioned<byte[]>> values);
	
	public void remove(K key, VectorClock vc);
	
	public Iterable<K> list();
	
	public void close();

}
