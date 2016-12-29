package mcsn.pad.pad_fs.storage.runnables;

import java.io.IOException;
import java.net.InetSocketAddress;

import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.PushMessage;
import mcsn.pad.pad_fs.message.ReplyMessage;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.transport.UDP;
import voldemort.versioning.Occurred;
import voldemort.versioning.Versioned;

public class PushHandler implements Runnable {

	final private SourceMessage pushMsg;
	final private LocalStore localStore;
	final private InetSocketAddress raddr;
	
	public PushHandler(SourceMessage pushMsg, LocalStore localStore) {
		this.pushMsg = pushMsg;
		this.localStore = localStore;
		this.raddr = new InetSocketAddress(pushMsg.addr, pushMsg.port);
	}

	@Override
	public void run() {
		PushMessage msg = (PushMessage) pushMsg.msg;
		Versioned<byte[]> v1 = msg.value;
		Versioned<byte[]> v2 = localStore.get(msg.key);
		//put v1 iff v1 > v2
		int occ = compare(v1, v2);
		if (occ == 1) {
			localStore.put(msg.key, msg.value);
		} else if (occ == 0) {
			//TODO handle concurrency case, maybe do nothing
		} else if (occ == -1) {
			try {
				//send REPLY msg to raddr
				UDP.send(new ReplyMessage(msg.key, v2), raddr);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private <T> int compare(Versioned<T> v1, Versioned<T> v2) {
		if (v2 == null)
			return 1;
        Occurred occurred = v1.getVersion().compare(v2.getVersion());
        if(occurred == Occurred.BEFORE)
            return -1;
        else if(occurred == Occurred.AFTER)
            return 1;
        else
            return 0;
	}

}
