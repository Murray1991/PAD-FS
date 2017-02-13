package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import mcsn.pad.pad_fs.common.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class MapDBStore extends LocalStore {
	
	private DB db;
	private HTreeMap<Serializable, List<Versioned<byte[]>>> map;
	
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();
	
	@SuppressWarnings("unchecked")
	public MapDBStore(String name) {
		super(name);
		
		db = DBMaker
				.fileDB(name)
				.closeOnJvmShutdown()
				.transactionEnable()			//to protect by crashes..
				.fileMmapEnableIfSupported()
				.executorEnable()
				.make();
		
		@SuppressWarnings("rawtypes")
		HTreeMap map = (HTreeMap<Serializable, List<Versioned<byte[]>>>) db
				.hashMap("principal")
				.counterEnable()
				.createOrOpen();
		
		this.map = map;
	}

	@Override
	public List<Versioned<byte[]>> get(Serializable key) {
		List<Versioned<byte[]>> res = null;
		rwLock.readLock().lock();
		try {
			res = map.get(key);
		} finally {
			rwLock.readLock().unlock();
		}
		return res;
	}

	@Override
	public void put(Serializable key, Versioned<byte[]> value) {
		rwLock.writeLock().lock();
		try {
			if ( pput(key, value) ) {
				db.commit();
			}
		} finally {
			rwLock.writeLock().unlock();
		}
	}
	
	/* fast fix for reply handler, it's ugly but doesn't commit for every put */
	public void put(List<Serializable> keys, List<Versioned<byte[]>> values) {
		if (keys.size() != values.size()) {
			throw new RuntimeException("put error");
		}
		
		rwLock.writeLock().lock();
		try {
			
			boolean b = false;
			for (int i=0; i<keys.size(); i++) {
				Serializable key = keys.get(i);
				Versioned<byte[]> value = values.get(i);
				b = b || pput(key, value);
			}
			
			if (b) {
				db.commit();
			}
			
		} finally {
			rwLock.writeLock().unlock();
		}
	}
	
	@Override
	public void remove(Serializable key, VectorClock vc) {
		put(key, new Versioned<byte[]>(null, vc));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterable<Serializable> list() {
		return map.keySet();
	}
	
	@Override
	public int size() {
		return map.keySet().size();
	}
	
	@Override
	public void close() {
	}
	
	private boolean pput(Serializable key, Versioned<byte[]> value) {
		List<Versioned<byte[]>> list = map.get(key);
		int occ = list == null ? 1 : Utils.compare(value, list.get(0));
		if ( occ == 1 )
			list = new ArrayList<>();
		if ( occ >= 0 ) {
			/* in order to avoid duplicates, remove first */
			list.remove(value);
			list.add(value);
			map.put(key, list);
		}
		return occ >= 0;
	}

}
