package mcsn.pad.pad_fs.storage;

import java.io.Serializable;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.List;
import java.util.Vector;

import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.membership.IMembershipService;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.client.GetMessage;
import mcsn.pad.pad_fs.message.client.ListMessage;
import mcsn.pad.pad_fs.message.client.PutMessage;
import mcsn.pad.pad_fs.message.client.RemoveMessage;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.transport.Transport;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * 
 * @author Leonardo Gazzarri
 *
 * this class resolves a client message
 * and writes / reads into/from the localStore
 * storing/updating vector clock info
 * 
 */

public class ResolveHelper {
	
	private IStorageService storageService;
	private IMembershipService membershipService;
	private LocalStore localStore;

	public ResolveHelper(IStorageService storageService, 
			IMembershipService membershipService, LocalStore localStore) {
		this.storageService = storageService;
		this.membershipService = membershipService;
		this.localStore = localStore;
	}

	public void resolveMessage(ClientMessage msg) {
		msg.status = Message.UNKNOWN;
		switch (msg.type) {
			case Message.GET:
				handleGet((GetMessage) msg);
				break;
			case Message.PUT:
				handlePut((PutMessage) msg);
				break;
			case Message.REMOVE:
				handleRemove((RemoveMessage) msg);
				break;
			case Message.LIST:
				handleList((ListMessage) msg);
				break;
			default:
				msg.status = Message.ERROR;
			}
		if (msg.status == Message.UNKNOWN)
			msg.status = Message.SUCCESS;
	}
	
	private void handleGet(GetMessage msg) {
		List<Versioned<byte[]>> values = localStore.get(msg.key);
		if (values != null && values.get(0).getValue() != null) {
			msg.values = new Vector<>();
			msg.values.addAll(values);
		} else {
			msg.status = Message.NOT_FOUND;
		}
	}
	
	/* only the coordinator put the version in the local store. */	
	private void handlePut(PutMessage msg) {
		msg.value = new Versioned<byte[]>(
				msg.value.getValue(), 
				storageService.incrementVectorClock());
		localStore.put(msg.key, msg.value);
	}
	
	/* remove only if the msg's version is greater than the local value's
	 * version but multicast anyway if "multicast" flag is true, only the
	 * coordinator can receive the packet with removeFlag setted to true */
	private void handleRemove(RemoveMessage msg) {
		
		if (msg.multicast) {
			msg.value = new Versioned<byte[]>(
					msg.value.getValue(),
					storageService.incrementVectorClock());
		}

		List<Versioned<byte[]>> list = localStore.get(msg.key);
		if (list == null || 1 == Utils.compare(msg.value, list.get(0))) {
			VectorClock vc = (VectorClock) msg.value.getVersion();
			localStore.remove(msg.key, vc);
		}

		DatagramSocket sck = null;
		try {
			
			if (msg.multicast) {
				msg.multicast = false;
				sck = new DatagramSocket();
				new Transport(sck, storageService)
					.multicast(msg, membershipService.getMembers());			
			}
			
		} catch (SocketException e) {
		} finally {
			if (sck != null)
				sck.close();
		}
	}
	
	private void handleList(ListMessage msg) {
		for (Serializable key : localStore.list())
			msg.addKey(key);
	}
}
