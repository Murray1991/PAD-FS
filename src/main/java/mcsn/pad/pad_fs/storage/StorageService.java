package mcsn.pad.pad_fs.storage;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

import junit.framework.Assert;
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
	
	private boolean isRunning;
	private StorageManager storageManager;
	private ReplicaManager replicaManager;
	private IMembershipService membershipService;
	private LocalStore localStore;
	private final int storageManagerPort = 3000; //TODO

	private VectorClock vectorClock;

	private int ID;

	public StorageService(IMembershipService membershipService, LocalStore localStore) {
		this.membershipService = membershipService;
		this.localStore = localStore;
		String host = membershipService.getMyself().host;
		storageManager = new StorageManager(this, host, storageManagerPort);
		replicaManager = new ReplicaManager(this, membershipService, 1000, host, storageManagerPort);
		
		//TODO check better ways to do ID...
		this.ID = Math.abs(host.hashCode()) % Short.MAX_VALUE;
		
		//Vector clock...
		this.vectorClock = new VectorClock();
		this.vectorClock.incrementVersion(this.ID, System.currentTimeMillis());
	}

	@Override
	public void start() {
		LOGGER.info(this + " -- start");
		isRunning = true;
		storageManager.start();
		replicaManager.start();
	}
	
	@Override
	public void shutdown() {
		LOGGER.info(this + " -- shutdown");
		isRunning = false;
		localStore.close(); //TODO check
		storageManager.interrupt();
		replicaManager.interrupt();
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}

	//TODO maybe better name is "HandleRequest"
	@Override
	public ClientMessage deliverMessage(ClientMessage msg) {
		Member coordinator = membershipService.getCoordinator((String) msg.key); //TODO check
		Member myself = membershipService.getMyself();
		ClientMessage rcvMsg = null;
		
		Assert.assertTrue( coordinator != null);
		Assert.assertTrue( myself != null);
		
		if (coordinator.equals(myself)) {
			resolveMessage(msg);
			rcvMsg = msg;
		} else {
			try {
				DatagramSocket socket = new DatagramSocket();
				Transport transport = new Transport(socket, this);
				transport.send(msg, new InetSocketAddress(coordinator.host, storageManagerPort)); 
				//UDP.send(msg, socket, new InetSocketAddress(coordinator.host, storageManagerPort)); 
				rcvMsg = (ClientMessage) transport.receive().msg; 
				//TODO check if rcvMsg is correct
				//TODO what if I don't receive the message? Do something with timeouts?
				socket.close();
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
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
		vectorClock.merge(vc);
		return vectorClock.clone();
	}
	
	@Override
	public VectorClock incrementVectorClock() {
		vectorClock.incrementVersion(getID(), System.currentTimeMillis());
		return vectorClock.clone();
	}

	@Override
	public VectorClock mergeAndIncrementVectorClock(VectorClock vc) {
		vectorClock.merge(vc);
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
				break;
			case Message.PUT:
				/* only Coordinator put the versioned value in the local store */
				msg.value = new Versioned<byte[]>(
						msg.value.getValue(), 
						incrementVectorClock());
				localStore.put(msg.key, msg.value);
				break;
			case Message.REMOVE:
				localStore.remove(msg.key);
				break;	
			case Message.LIST:
				//TODO implement LIST...
				break;
			default:
				msg.status = Message.ERROR;
			}
		if (msg.status == Message.UNKNOWN)
			msg.status = Message.SUCCESS;
	}
}
