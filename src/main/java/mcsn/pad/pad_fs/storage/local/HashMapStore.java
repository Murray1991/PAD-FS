package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.Versioned;

public class HashMapStore extends LocalStore {
	
	private ConcurrentHashMap<Serializable, Versioned<byte[]>> hmap;
	
	private HashMapStore(String name, String prepend) {
		super(prepend+name);
		hmap = new ConcurrentHashMap<>();
	}
	
	public HashMapStore(String name) {
		super(name, new HashMapStore(name, "concurrent_"));
		hmap = new ConcurrentHashMap<>();
	}
	
	@Override
	public Versioned<byte[]> get(Serializable key) {
		return hmap.get(key);
	}

	@Override
	public void put(Serializable key, Versioned<byte[]> value) {
		hmap.put(key, value);
	}
	
	@Override
	public void remove(Serializable key) {
		hmap.remove(key);
	}

	@Override
	public Iterable<Serializable> list() {
		return hmap.keySet();
	}
	
	public int size() {
		return hmap.size();
	}

	@Override
	public void close() {
	}
}
