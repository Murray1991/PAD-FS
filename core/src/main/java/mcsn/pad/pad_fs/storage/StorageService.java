package mcsn.pad.pad_fs.storage;

import java.io.IOException;
import java.io.Serializable;
import java.lang.Thread.State;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.logging.Logger;

import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.membership.IMembershipService;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.transport.Transport;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class StorageService implements IStorageService {
	
	private final static Logger LOGGER = Logger.getLogger(StorageService.class.getName());
	
	private boolean isRunning = false;
	private StorageManager storageManager;
	private ReplicaManager replicaManager;
	private IMembershipService membershipService;
	private LocalStore localStore;
	private LocalStore concurrentLocalStore;
	//TODO See if I can set the storageManagerPort in the config file
	private final int storageManagerPort = 3000; 

	private VectorClock vectorClock;

	private int ID;

	private String host;


	public StorageService(IMembershipService membershipService, LocalStore localStore) {
		Assert.assertNotNull(localStore.getNext());
		this.membershipService = membershipService;
		this.localStore = localStore;
		this.concurrentLocalStore = localStore.getNext();
		this.host = membershipService.getMyself().host;
		storageManager = new StorageManager(this, host, storageManagerPort);
		replicaManager = new ReplicaManager(this, membershipService, 1000, host, storageManagerPort);
		
		//TODO Maybe better way to get an ID
		this.ID = Math.abs(host.hashCode()) % Short.MAX_VALUE;
		
		//Vector clock...
		this.vectorClock = new VectorClock();
		this.vectorClock.incrementVersion(this.ID, System.currentTimeMillis());
	}

	@Override
	public void start() {
		LOGGER.info(this + " -- start");
		State stateS = storageManager.getState();
		State stateR = replicaManager.getState();
		if (! stateS.equals(State.NEW) ) {
			storageManager = new StorageManager(this, host, storageManagerPort);
		}
		if (! stateR.equals(State.NEW)) {
			replicaManager = new ReplicaManager(this, membershipService, 1000, host, storageManagerPort);
		}
		if (!isRunning) {
			isRunning = true;
			storageManager.start();
			replicaManager.start();
		}
	}
	
	@Override
	public void shutdown() {
		LOGGER.info(this + " -- shutdown");
		if (isRunning) {
			isRunning = false;
			replicaManager.interrupt();
			storageManager.interrupt();
			localStore.close(); 
		}
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}

	@Override
	public ClientMessage deliverMessage(ClientMessage msg) {
		Member coordinator = membershipService.getCoordinator(
				msg.key != null ? (String) msg.key : "");
		Member myself = membershipService.getMyself();
		ClientMessage rcvMsg = null;
		if (coordinator.equals(myself) || msg.type == Message.REMOVE && !msg.removeFlag || msg.type == Message.LIST) {
			resolveMessage(msg);
			rcvMsg = msg;
		} else {
			DatagramSocket socket = null;
			try {
				socket = new DatagramSocket();
				Transport transport = new Transport(socket, this);
				transport.send(msg, new InetSocketAddress(coordinator.host, storageManagerPort)); 
				rcvMsg = (ClientMessage) transport.receive().msg; 
				//TODO check if rcvMsg is correct and handle 
				//the case in which I don't receive the message (timeouts)
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			} finally {
				if (socket != null) 
					socket.close();
			}
		}
		return rcvMsg;
	}
	
	@Override
	public synchronized VectorClock getVectorClock() {
		return vectorClock.clone();
	}
	
	@Override
	public synchronized VectorClock mergeVectorClock(VectorClock vc) {
		vectorClock = vectorClock.merge(vc);
		return vectorClock.clone();
	}
	
	@Override
	public synchronized VectorClock incrementVectorClock() {
		vectorClock.incrementVersion(getID(), System.currentTimeMillis());
		return vectorClock.clone();
	}

	@Override
	public synchronized VectorClock mergeAndIncrementVectorClock(VectorClock vc) {
		vectorClock = vectorClock.merge(vc);
		vectorClock.incrementVersion(getID(), System.currentTimeMillis());
		return vectorClock.clone();
	}
	
	@Override
	public int getID() {
		return ID;
	}

	@Override
	public LocalStore getLocalStore() {
		return localStore;
	}

	@Override
	public int getStoragePort() {
		return storageManager.getStoragePort();
	}

	@Override
	public InetAddress getStorageAddress() {
		return storageManager.getStorageAddress();
	}
	
	@Override
	public String toString() {
		return "[StorageService@"+ this.membershipService.getMyself().id + "@" + getID() + "]";
	}
	
	private void resolveMessage(ClientMessage msg) {
		msg.status = Message.UNKNOWN;
		switch (msg.type) {
			case Message.GET:
				msg.value = localStore.get(msg.key);
				msg.addValue(concurrentLocalStore.get(msg.key));
				if (msg.value == null)
					msg.status = Message.NOT_FOUND;
				break;
			case Message.PUT:
				/* only the coordinator put the version
				 * in the local store. TODO check if ok... */
				msg.value = new Versioned<byte[]>(
						msg.value.getValue(), 
						incrementVectorClock());
				localStore.put(msg.key, msg.value);
				concurrentLocalStore.remove(msg.key);
				break;
			case Message.REMOVE:
				/* remove only if the msg's version
				 * is greater than the local value's
				 * version but multicast anyway if
				 * removeFlag is true, only the
				 * coordinator can receive the
				 * packet with removeFlag setted
				 * to true */
				if (msg.removeFlag) {
					msg.value = new Versioned<byte[]>(
							msg.value.getValue(), 
							incrementVectorClock());
				}
				Versioned<byte[]> v1= msg.value;
				Versioned<byte[]> v2 = localStore.get(msg.key);
				if ( Utils.compare(v1, v2) == 1 ) {
					localStore.remove(msg.key);
					concurrentLocalStore.remove(msg.key);
				} 
				if (msg.removeFlag) {
					msg.removeFlag = false;
					DatagramSocket sck = null;
					try {
						sck = new DatagramSocket();
						new Transport(sck, this).multicast(msg, membershipService.getMembers());
					} catch (SocketException e) {
					} finally {
						if (sck != null)
							sck.close();
					}
				}
				break;
			case Message.LIST:
				for (Serializable key : localStore.list())
					msg.addKey(key);
				break;
			default:
				msg.status = Message.ERROR;
			}
		if (msg.status == Message.UNKNOWN)
			msg.status = Message.SUCCESS;
	}
}
