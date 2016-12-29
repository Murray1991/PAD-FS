package mcsn.pad.pad_fs.storage.runnables;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;

import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.transport.UDP;

public class UpdateHandler implements Runnable {
	
	final private InetSocketAddress partner;
	final private LocalStore localStore;

	public UpdateHandler(InetSocketAddress partner, LocalStore localStore) {
		this.partner = partner;
		this.localStore = localStore;
	}

	@Override
	public void run() {
		//send all local <key,value> pairs to the  partner
		//TODO send less messages... (in one message more <key,value> pairs, or better only keys)
		for (Serializable key : localStore.list()) {
			Message msg = new Message(Message.PUSH, key, localStore.get(key));
			try {
				UDP.send(msg, partner);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
