package mcsn.pad.pad_fs.storage.runnables;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import mcsn.pad.pad_fs.message.ReplyMessage;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * 
 * @author Leonardo Gazzarri
 *
 * It handles a Reply message: the receiver put in the local store
 * the <key,value> tuples received if in the local store there is
 * an older version for the value associated to the key.
 * In case of concurrency the new value will be stored in the 
 * concurrentLocalStore, if this store is not empty for the key
 * a simple timestamp-based strategy is adopted for the resolution.
 */
public class ReplyHandler implements Runnable {

    static final Logger logger = Logger.getLogger(ReplyHandler.class);

	final private SourceMessage replyMsg;
	final private LocalStore localStore;

	public ReplyHandler(SourceMessage replyMsg, LocalStore localStore) {
		this.replyMsg = replyMsg;
		this.localStore = localStore;
	}
	
	public ReplyHandler(SourceMessage replyMsg, IStorageService storageService) {
		this(replyMsg, storageService.getLocalStore());
	}

	@Override
	public void run() {
		
		long start = 0;
		if (logger.isTraceEnabled()) {
			logger.trace(Thread.currentThread().getId() + ": replyHandler");
			start = System.nanoTime(); 
		}
		
		ReplyMessage msg = (ReplyMessage) replyMsg.msg;
		
		Vector<Serializable> replyKeys = msg.keys;
		Vector<List<Versioned<byte[]>>> replyValues = msg.values;
		
		List<Serializable> keys = new ArrayList<>();
		List<Versioned<byte[]>> values = new ArrayList<>();
		
		for (int i = 0; i < replyKeys.size(); i++) {
			Serializable key = replyKeys.get(i);
			List<Versioned<byte[]>> l1 = replyValues.get(i);
			List<Versioned<byte[]>> l2 = localStore.get(key);
			
			HashSet<Version> hs = new HashSet<>();
			if ( l2 != null ) {
				List<Version> versions = l2
						.stream()
						.map( v->v.getVersion() )
						.collect(Collectors.toList());
				hs.addAll(versions);
			}
			
			/* add if at least one of the versions in l1 is not present in the local store */
			for (Versioned<byte[]> v : l1) {
				if (!hs.contains(v.getVersion())) {
					keys.add(key);
					values.add(v);
				}
			}
			
		}
		
		/* put all together in the store */
		localStore.put(keys, values);
		if (logger.isTraceEnabled()) {
			long delta = System.nanoTime() - start;
			logger.trace(Thread.currentThread().getId() + "time elapsed to process reply message: " + TimeUnit.NANOSECONDS.toMillis(delta));
			//logger.trace(Thread.currentThread().getId() + "reply message's size: #bytes = " + Utils.sizeof(msg) + ", #keys = " + msg.keys.size());
		}
	}
}
