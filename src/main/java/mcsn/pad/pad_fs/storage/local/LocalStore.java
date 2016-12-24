package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import java.util.Map;

import mcsn.pad.pad_fs.storage.Store;
import voldemort.versioning.Versioned;

public abstract class LocalStore implements Store<Serializable, byte[]> {
	
	private String name;
	
	public LocalStore(String name) {
		this.name = name;
	}

	@Override
	public Versioned<byte[]> get(Serializable key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void put(Serializable key, Versioned<byte[]> value) {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void remove(Serializable key) {
		// TODO
	}

	@Override
	public Map<Serializable, Versioned<byte[]>> list(Iterable<Serializable> keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}
	
	@Override
	public String toString() {
		return "[LocalStore@"+this.name+"]";
	}
	
}
