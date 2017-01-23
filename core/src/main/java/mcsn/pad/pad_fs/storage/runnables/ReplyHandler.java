package mcsn.pad.pad_fs.storage.runnables;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Vector;

import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.message.ReplyMessage;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.TimeBasedInconsistencyResolver;
import voldemort.versioning.VectorClock;
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

	final private SourceMessage replyMsg;
	final private LocalStore localStore;
	final private LocalStore concurrentLocalStore;

	public ReplyHandler(SourceMessage replyMsg, LocalStore localStore) {
		this.replyMsg = replyMsg;
		this.localStore = localStore;
		this.concurrentLocalStore = localStore.getNext();
		Assert.assertNotNull(concurrentLocalStore);
	}
	
	public ReplyHandler(SourceMessage replyMsg, IStorageService storageService) {
		this(replyMsg, storageService.getLocalStore());
	}

	@Override
	public void run() {
		ReplyMessage msg = (ReplyMessage) replyMsg.msg;
		Vector<Serializable> replyKeys = msg.keys;
		Vector<Versioned<byte[]>> replyValues = msg.values;
		for (int i = 0; i < replyKeys.size(); i++) {
			Serializable key = replyKeys.get(i);
			Versioned<byte[]> v1 = replyValues.get(i);
			Versioned<byte[]> v2 = localStore.get(key);
			int occ = Utils.compare(v1, v2);
			if ( occ == 1 && v1 != null) {
				localStore.put(key, v1);
				concurrentLocalStore.remove(key);
			} else if ( occ == 0 && !v1.equals(v2) ) {
				concurrentResolution(key, v1, v2);
			}
		}
	}
	
	private void concurrentResolution(Serializable key, Versioned<byte[]> value1, Versioned<byte[]> value2) {
		/* if the two array values are equal, merge their VCs 
		 * and store the new versioned value in localStore */
		if (Arrays.equals(value1.getValue(), value2.getValue())) {
			VectorClock vc1 = (VectorClock) value1.getVersion();
			VectorClock vc2 = (VectorClock) value2.getVersion();
			Versioned<byte[]> value = new Versioned<byte[]>(
					value1.getValue(), 
					vc1.merge(vc2));
			localStore.put(key, value);	
			return;
		}
		/* store the value according to a timestamp-based strategy */
		Versioned<byte[]> v1 = value1;
		Versioned<byte[]> v2 = concurrentLocalStore.get(key);
		Versioned<byte[]> toInsert = v2 == null ? value1 : 
					new TimeBasedInconsistencyResolver<byte[]>()
						.resolveConflicts( Arrays.asList(v1, v2) )
						.get(0);		
		concurrentLocalStore.put(key, toInsert);
	}
}
