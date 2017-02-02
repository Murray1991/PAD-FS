package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import java.util.List;

import mcsn.pad.pad_fs.storage.local.IStore;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public abstract class LocalStore implements IStore<Serializable, byte[]> {
	
	private String name;
	
	public LocalStore(String name) {
		this.name = name;
	}
	
	public int size() {
		return 0;
	}

	@Override
	public List<Versioned<byte[]>> get(Serializable key) {
		return null;
	}

	@Override
	public void put(Serializable key, Versioned<byte[]> value) {
	}
	
	@Override
	public void remove(Serializable key, VectorClock vc) {
	}
	
	@Override
	public void delete(Serializable key) {
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
