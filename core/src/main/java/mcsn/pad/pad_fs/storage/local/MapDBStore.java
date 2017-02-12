package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import mcsn.pad.pad_fs.common.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class MapDBStore extends LocalStore {
	
	private DB db;
	private BTreeMap<Serializable, List<Versioned<byte[]>>> map;
	
	/*
	private DB dbCache;
	private HTreeMap<Serializable, List<Versioned<byte[]>>> mapCache;
	*/
	
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
		BTreeMap map = (BTreeMap<Serializable, List<Versioned<byte[]>>>) db
				.treeMap("principal")
				.counterEnable()
				.createOrOpen();
		
		this.map = map;
		
		/* faster approach but it does not commit the inDisk files
		 * TODO investigate..
		 
		dbCache = DBMaker
				.memoryDB()
				.make();
		
		mapCache = dbCache
				.hashMap("inMemory")
				.expireOverflow(map)
				.expireAfterCreate(1000)
				.expireAfterUpdate(1000)
				.expireAfterGet(1000)
				//.expireMaxSize(3*1024*1024) //3MB
				.expireExecutor(Executors.newScheduledThreadPool(2))
				.expireExecutorPeriod(1000)
				.create();
				
		*/
	}

	@Override
	public List<Versioned<byte[]>> get(Serializable key) {
		return map.get(key);
	}

	@Override
	synchronized public void put(Serializable key, Versioned<byte[]> value) {
		if ( pput(key, value) ) {
			db.commit();
		}
	}
	
	/* fast fix for reply handler, it's ugly but doesn't commit for every put */
	synchronized public void put(List<Serializable> keys, List<Versioned<byte[]>> values) {
		if (keys.size() != values.size()) {
			throw new RuntimeException("put error");
		}
		
		boolean b = false;
		for (int i=0; i<keys.size(); i++) {
			Serializable key = keys.get(i);
			Versioned<byte[]> value = values.get(i);
			b = b || pput(key, value);
		}
		
		if (b) {
			db.commit();
		}
	}
	
	@Override
	synchronized public void remove(Serializable key, VectorClock vc) {
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
