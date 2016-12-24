package mcsn.pad.pad_fs.storage.remote;

import java.io.Serializable;
import java.util.Map;

import mcsn.pad.pad_fs.storage.Store;
import voldemort.versioning.Versioned;

public abstract class RemoteStore implements Store<Serializable, byte[]> {
	
	public static final int MAX_PACKET_SIZE = 102400;
	
	public static final int PUT = 0;
	public static final int GET = 1;
	public static final int LIST = 2;
	public static final int REMOVE = 3;

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
	public void remove(Serializable key){
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


}
