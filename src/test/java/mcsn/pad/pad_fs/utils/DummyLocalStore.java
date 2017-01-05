package mcsn.pad.pad_fs.utils;

import java.io.Serializable;
import java.util.HashMap;

import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.Versioned;

public class DummyLocalStore extends LocalStore {
	
	private HashMap<Serializable, Versioned<byte[]>> hmap;
	
	public DummyLocalStore(String name) {
		super(name);
		hmap = new HashMap<>();
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

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}
}
