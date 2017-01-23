package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import mcsn.pad.pad_fs.storage.local.IStore;
import voldemort.versioning.Versioned;

public abstract class LocalStore implements IStore<Serializable, byte[]> {
	
	private String name;
	private LocalStore nextLocalStore;
	
	public LocalStore(String name) {
		this.name = name;
	}
	
	public LocalStore(String name, LocalStore nextLocalStore) {
		this.name = name;
		this.nextLocalStore = nextLocalStore;
	}
	
	public LocalStore getNext() {
		return nextLocalStore;
	}
	
	public int size() {
		return 0;
	}

	@Override
	public Versioned<byte[]> get(Serializable key) {
		return null;
	}

	@Override
	public void put(Serializable key, Versioned<byte[]> value) {
	}
	
	@Override
	public void remove(Serializable key) {
	}

	@Override
	public Iterable<Serializable> list() {
		return null;
	}
	
	@Override
	public void close() {
	}
	
	@Override
	public String toString() {
		return "[LocalStore@"+this.name+"]";
	}

}
