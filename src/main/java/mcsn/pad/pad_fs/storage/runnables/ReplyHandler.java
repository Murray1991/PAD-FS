package mcsn.pad.pad_fs.storage.runnables;

import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.Occurred;
import voldemort.versioning.Versioned;

public class ReplyHandler implements Runnable {

	final private SourceMessage replyMsg;
	final private LocalStore localStore;

	public ReplyHandler(SourceMessage replyMsg, LocalStore localStore) {
		this.replyMsg = replyMsg;
		this.localStore = localStore;
	}

	@Override
	public void run() {
		Message msg = replyMsg.msg;
		Versioned<byte[]> v1 = msg.value;
		Versioned<byte[]> v2 = localStore.get(msg.key);
		Occurred occ = v1.getVersion().compare(v2.getVersion());
		if (occ == Occurred.AFTER) {
			localStore.put(msg.key, msg.value);
		}
	}
}
