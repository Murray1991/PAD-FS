package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class HashMapStore extends LocalStore {
	
	private ConcurrentHashMap<Serializable, List<Versioned<byte[]>>> hmap;
	
	public HashMapStore(String name) {
		super(name);
		hmap = new ConcurrentHashMap<>();
	}
	
	@Override
	public List<Versioned<byte[]>> get(Serializable key) {
		return hmap.get(key);
	}

	@Override
	synchronized public void put(Serializable key, Versioned<byte[]> value) {
		synchronized(this) {
			List<Versioned<byte[]>> list = hmap.get(key);
			int occ = list == null ? 1 : Utils.compare(value, list.get(0));
			if ( occ == 1 ) 
				list = new ArrayList<>();
			if ( occ >= 0 ) {
				/* in order to avoid duplicates, remove first */
				list.remove(value);
				list.add(value);
				hmap.put(key, list);
			}
		}
	}
	
	@Override
	synchronized public void remove(Serializable key, VectorClock vc) {
		put(key, new Versioned<byte[]>(null, vc));
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
