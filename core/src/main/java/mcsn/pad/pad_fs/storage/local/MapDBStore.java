package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import voldemort.versioning.Versioned;

public class MapDBStore extends LocalStore {
	
	private DB db;
	private HTreeMap<Serializable, Versioned<byte[]>> map;
	
	@SuppressWarnings("unchecked")
	private MapDBStore(String name, String append) {
		super(name+append);
		db = DBMaker.fileDB(name+append).make();
		map = (HTreeMap<Serializable, Versioned<byte[]>>) 
				db.hashMap("concurrent").createOrOpen();
	}
	
	@SuppressWarnings("unchecked")
	public MapDBStore(String name) {
		super(name, new MapDBStore(name, ".concurrent"));
		db = DBMaker.fileDB(name).make();
		map = (HTreeMap<Serializable, Versioned<byte[]>>) 
				db.hashMap("principal").createOrOpen();
	}

	@Override
	public Versioned<byte[]> get(Serializable key) {
		return (Versioned<byte[]>) map.get(key);
	}

	@Override
	public void put(Serializable key, Versioned<byte[]> value) {
		map.put(key, value);
		db.commit();
	}
	
	@Override
	public void remove(Serializable key) {
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
