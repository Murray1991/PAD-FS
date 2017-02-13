package mcsn.pad.pad_fs.storage.runnables;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

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

    static final Logger logger = Logger.getLogger(PushHandler.class);

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
		Map<Serializable, List<Versioned<byte[]>>> replyMap = new HashMap<>();
		
		for (int i = 0; i < msg.keys.size(); i++) {
			Serializable key = msg.keys.get(i);
			List<Versioned<byte[]>> l1 = msg.values.get(i);
			List<Versioned<byte[]>> l2 = localStore.get(key);
			Versioned<byte[]> v1 = l1.get(0);
			Versioned<byte[]> v2 = l2 != null ? l2.get(0) : null;
			
			int occ = Utils.compare(v1, v2);
			if (occ == 1) {
				pullKeys.add(key);
			} else if (occ == -1) {
				replyMap.put(key, l2);
			} else if (occ == 0 && !new HashSet<>(l2).containsAll(l1)) {
				/* add for the pull messages in order to ask the values 
				 * and handle the concurrency case later in the ReplyHandler */
				pullKeys.add(key);
			}
		}
		
		/* send pull message to ask the missing/not-updated keys */
		//TODO send more than one PullMessage if all the keys occupy too much space or a threshold number
		if (pullKeys.size() > 0) {
			try {
				Transport transport = new Transport(storageService);
				transport.send(new PullMessage(pullKeys, storageAddr, storagePort), raddr);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		/* send reply messages for the local-updated keys */
		//TODO send more than one ReplyMessage if all the keys occupy too much space or a threshold number
		if (replyMap.size() > 0) {
			try {
				Transport transport = new Transport(storageService);
				transport.send(new ReplyMessage(replyMap), raddr);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
