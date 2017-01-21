package mcsn.pad.pad_fs.storage.runnables;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.message.PullMessage;
import mcsn.pad.pad_fs.message.PushMessage;
import mcsn.pad.pad_fs.message.ReplyMessage;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.transport.Transport;
import voldemort.versioning.Versioned;

/**
 *
 * @author Leonardo Gazzarri
 *
 * It handles a Push message: the receiver of this message send back
 * to the sender a collection of keys for which the local values
 * are not updated (it builds and sends back one or more Pull messages)
 * and a collection of <key, value> tuples for which the local values
 * have a recent version wrt the received keys (it builds and sends back
 * one or more Reply messages)
 */
public class PushHandler implements Runnable {

	final private LocalStore localStore;
	final private InetSocketAddress raddr;
	final private InetAddress storageAddr;
	final private int storagePort;
	private IStorageService storageService;
	private PushMessage msg;
	
	public PushHandler(SourceMessage pushMsg, IStorageService storageService) {
		this.msg = (PushMessage) pushMsg.msg;
		this.storageAddr = storageService.getStorageAddress();
		this.storagePort = storageService.getStoragePort();
		this.storageService = storageService;
		this.localStore = storageService.getLocalStore();
		this.raddr = new InetSocketAddress(msg.storageAddr, msg.storagePort);
	}

	@Override
	public void run() {
		ArrayList<Serializable> pullKeys = new ArrayList<>();
		ArrayList<Serializable> replyKeys = new ArrayList<>();
		ArrayList<Versioned<byte[]>> replyValues = new ArrayList<>();
		
		for (int i = 0; i < msg.keys.size(); i++) {
			Serializable key = msg.keys.get(i);
			Versioned<byte[]> v1 = msg.values.get(i);
			Versioned<byte[]> v2 = localStore.get(key);
			
			int occ = Utils.compare(v1, v2);
			if (occ == 1) {
				pullKeys.add(key);
			} else if (occ == -1) {
				replyKeys.add(key);
				replyValues.add(v2);
			} else if (occ == 0 && !v1.equals(v2)) {
				//genero dei messaggi PULL per richiedere i valori
				//gestione della concorrenza nel ReplyHandler
				pullKeys.add(key);
			}
		}
		
		//Send pull message to ask the missing/not-updated keys
		//TODO send more than one PullMessage if all the keys occupy too much space or a threshold number
		if (pullKeys.size() > 0) {
			//System.out.println("pullKeys size: " + pullKeys.size());
			try {
				Transport transport = new Transport(storageService);
				transport.send(new PullMessage(pullKeys, storageAddr, storagePort), raddr);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		//Send reply messages for the local-updated keys
		//TODO send more than one ReplyMessage if all the keys occupy too much space or a threshold number
		if (replyKeys.size() > 0) {
			//System.out.println("replyKeys size: " + replyKeys.size());
			try {
				Transport transport = new Transport(storageService);
				transport.send(new ReplyMessage(replyKeys, replyValues), raddr);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
