package mcsn.pad.pad_fs.storage.local;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import mcsn.pad.pad_fs.common.Pair;
import mcsn.pad.pad_fs.common.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class ActiveMapDBStore extends LocalStore {
	
    static final Logger logger = Logger.getLogger(ActiveMapDBStore.class);
	
	private DB db;
	private HTreeMap<Serializable, List<Versioned<byte[]>>> map;
	private ConcurrentLinkedQueue<Pair<Serializable, Versioned<byte[]>>> cq;
	
	
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();
    
    /* it handles puts and commitments*/
    private Thread putHandler;
    /* putHandler commits the changes every commitPutThresh puts */
	private final int commitPutThresh = 50;
	/* putHandler always commits the changes after ~1500 milliseconds from last put*/
	private final long commitTimeThresh = 1500;
	/* if no puts are present, sleep a little bit*/
	private long waitTime = 200;
	
	@SuppressWarnings("unchecked")
	public ActiveMapDBStore(String name) {
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
		
		this.cq = new ConcurrentLinkedQueue<>();
		
		/**
		 * This thread updates in background the database
		 * exploiting puts and commitments. The thread
		 * gets the puts from a concurrent queue
		 */
		this.putHandler = new Thread(() -> {
			
			System.out.println("putHandler starts");
			
			long start = System.nanoTime();
			int count = 0;
			
			while (true) {
				try {
					/* commit if number of puts is greater than the thresh */
					if (count > commitPutThresh ) {
						logger.trace("commit");
						count = 0;
						commit();
					}
					
					Pair<Serializable,Versioned<byte[]>> p = cq.poll();
					/* if the polled pair is not null, put in map */
					if (p != null) {
						start = System.nanoTime();
						pput(p.key, p.value);
						count++;
					}
					
					/* check if some time is passed from the last put */
					if (count > 0 && p == null &&
							commitTimeThresh < 
							TimeUnit.NANOSECONDS.toMillis(
									System.nanoTime()-start)) {
						logger.trace("commit for time firing");
						start = System.nanoTime();
						count = 0;
						commit();
					} else if (p == null) {
						/* sleep if nothing to commit */
						Thread.sleep(waitTime);
					}
				} catch (InterruptedException e) {
				}	
			}
		});
		
		this.putHandler.start();
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
		cq.add(new Pair<>(key, value));
	}
	
	@Override
	public void put(List<Serializable> keys, List<Versioned<byte[]>> values) {
		if (keys.size() != values.size()) {
			throw new RuntimeException("put error");
		}
		
		for (int i=0; i<keys.size(); i++)
			put(keys.get(i), values.get(i));
		
	}
	
	@Override
	public void remove(Serializable key, VectorClock vc) {
		put(key, new Versioned<byte[]>(null, vc));
	}
	
	/* puts are serialized by the putHandler, no need to synchronize them with
	 * commitments or gets */
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
	
	/* need to synchronize commitments with gets */
	private void commit() {
		rwLock.writeLock().lock();
		try {
			db.commit();
		} finally {
			rwLock.writeLock().unlock();
		}
	}
	
	@Override
	public Iterable<Serializable> list() {

		List<Serializable> keys = new ArrayList<>();
		
		map.forEach( (key, list) -> {
			if (list.get(0).getValue() != null)
				keys.add(key);
		});
		
		return keys;
	}
	
	@Override
	public int size() {
		
		return map.keySet().size();
	}
	
	@Override
	public void close() {
	}

}
