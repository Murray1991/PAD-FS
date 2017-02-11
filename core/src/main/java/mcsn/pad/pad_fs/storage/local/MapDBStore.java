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
	
	//onMemory
	private DB db;
	private BTreeMap<Serializable, List<Versioned<byte[]>>> map;
	
	@SuppressWarnings("unchecked")
	public MapDBStore(String name) {
		super(name);
		
		db = DBMaker
				.fileDB(name)
				.make();
		
		map = (BTreeMap<Serializable, List<Versioned<byte[]>>>) db
				.treeMap("principal")
				.createOrOpen();
		
		/* map = (HTreeMap<Serializable, List<Versioned<byte[]>>>) db
				.hashMap("principal")
				.createOrOpen(); */
		
	}

	@Override
	public List<Versioned<byte[]>> get(Serializable key) {
		return map.get(key);
	}

	@Override
	public void put(Serializable key, Versioned<byte[]> value) {
		/* TODO race conditions here? */
		List<Versioned<byte[]>> list = map.get(key);
		int occ = list == null ? 1 : Utils.compare(value, list.get(0));
		if ( occ == 1 )
			list = new ArrayList<>();
		if ( occ >= 0 ) {
			/* in order to avoid duplicates, remove first */
			list.remove(value);
			list.add(value);
			map.put(key, list);
			db.commit();
		}
	}
	
	@Override
	public void remove(Serializable key, VectorClock vc) {
		put(key, new Versioned<byte[]>(null, vc));
	}
	
	@Override
	public void delete(Serializable key) {
		map.remove(key);
		db.commit();
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

}
