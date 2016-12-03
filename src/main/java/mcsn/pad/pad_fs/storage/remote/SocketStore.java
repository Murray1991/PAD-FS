package mcsn.pad.pad_fs.storage.remote;

import java.io.Serializable;
import java.util.Map;

import voldemort.versioning.Versioned;

//TODO TCP store
public class SocketStore extends RemoteStore {
	
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
