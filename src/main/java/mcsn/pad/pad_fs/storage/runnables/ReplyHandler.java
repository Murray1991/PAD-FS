package mcsn.pad.pad_fs.storage.runnables;

import java.io.Serializable;
import java.util.Vector;

import mcsn.pad.pad_fs.common.Utils;
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
		Vector<Versioned<byte[]>> replyValues = msg.values;
		for (int i = 0; i < replyKeys.size(); i++) {
			Serializable key = replyKeys.get(i);
			Versioned<byte[]> v1 = replyValues.get(i);
			Versioned<byte[]> v2 = localStore.get(key);
			if ( Utils.compare(v1, v2) == 1 ) {
				localStore.put(key, v1);
			}
		}
	}
}
