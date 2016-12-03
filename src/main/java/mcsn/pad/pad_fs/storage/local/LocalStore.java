package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import java.util.Map;

import mcsn.pad.pad_fs.storage.Store;
import voldemort.versioning.Versioned;

public abstract class LocalStore implements Store<Serializable, byte[]> {

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
	public Map<Serializable, Versioned<byte[]>> list(Iterable<Serializable> keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	
}
