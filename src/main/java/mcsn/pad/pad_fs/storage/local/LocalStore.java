package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import mcsn.pad.pad_fs.storage.local.IStore;
import voldemort.versioning.Versioned;

public abstract class LocalStore implements IStore<Serializable, byte[]> {
	
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
	public Iterable<Serializable> list() {
		// TODO Auto-generated method stub
		return null;
	}

	public int size() {
		// TODO Auto-generated method stub
		return 0;
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
