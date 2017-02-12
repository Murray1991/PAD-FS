package mcsn.pad.pad_fs.storage.runnables;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import mcsn.pad.pad_fs.message.ReplyMessage;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
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

	public ReplyHandler(SourceMessage replyMsg, LocalStore localStore) {
		this.replyMsg = replyMsg;
		this.localStore = localStore;
	}
	
	public ReplyHandler(SourceMessage replyMsg, IStorageService storageService) {
		this(replyMsg, storageService.getLocalStore());
	}

	@Override
	public void run() {
		ReplyMessage msg = (ReplyMessage) replyMsg.msg;
		
		Vector<Serializable> replyKeys = msg.keys;
		Vector<List<Versioned<byte[]>>> replyValues = msg.values;
		
		List<Serializable> keys = new ArrayList<>();
		List<Versioned<byte[]>> values = new ArrayList<>();
		
		for (int i = 0; i < replyKeys.size(); i++) {
			Serializable key = replyKeys.get(i);
			List<Versioned<byte[]>> l2 = localStore.get(key);
			Versioned<byte[]> v1 = replyValues.get(i).get(0);
			Versioned<byte[]> v2 = l2 != null ? l2.get(0) : null;
			if (v1 != null && !v1.equals(v2)) {
				for (Versioned<byte[]> v : replyValues.get(i)) {
					keys.add(key);
					values.add(v);
				}
			}
		}
		
		localStore.put(keys, values);
	}
}
