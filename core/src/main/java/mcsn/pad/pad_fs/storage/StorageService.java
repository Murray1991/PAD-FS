package mcsn.pad.pad_fs.storage;

import java.io.IOException;
import java.io.Serializable;
import java.lang.Thread.State;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

import org.junit.Assert;

import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.membership.IMembershipService;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.client.GetMessage;
import mcsn.pad.pad_fs.message.client.ListMessage;
import mcsn.pad.pad_fs.message.client.PutMessage;
import mcsn.pad.pad_fs.message.client.RemoveMessage;
import mcsn.pad.pad_fs.message.client.RoutableClientMessage;
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
	//TODO See if I can set the storageManagerPort in the config file
	private final int storageManagerPort = 3000; 

	private VectorClock vectorClock;

	private int ID;

	private String host;


	public StorageService(IMembershipService membershipService, LocalStore localStore) {
		this.membershipService = membershipService;
		this.localStore = localStore;
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
		
		/* check if it's routable */
		Member myself = membershipService.getMyself();
		Member coordinator = null;
		if ( msg instanceof RoutableClientMessage ) {
			RoutableClientMessage rmsg = (RoutableClientMessage) msg;
			String key = (String) rmsg.key;
			coordinator = membershipService.getCoordinator(key);
			Assert.assertNotNull(coordinator);
		}
		
		/* check if it's a remove message and its multicast property */
		boolean isRemove = false;
		boolean multicast = false;
		if ( msg instanceof RemoveMessage ) {
			isRemove = true;
			multicast = ((RemoveMessage) msg).multicast;
		}
		
		/* resolve the message or route to the coordinator */
		ClientMessage rcvMsg = null;
		if (coordinator != null && coordinator.equals(myself) || coordinator == null || isRemove && !multicast) {
			/* case msg is a LIST or a REMOVE without multicast 
			 * or the actual node is the coordinator for the key */
			resolveMessage(msg);
			rcvMsg = msg;
			
		} else {
			/* case msg is a RoutableMessage and the actual node 
			 * is not the coordinator for this key */
			DatagramSocket socket = null;
			try {
				
				socket = new DatagramSocket();
				Transport transport = new Transport(socket, this);
				transport.send(msg, new InetSocketAddress(coordinator.host, storageManagerPort));
				try {
					/* receive with timeout */
					rcvMsg = (ClientMessage) transport.receive(1000).msg;
				} catch (SocketTimeoutException e) {
					System.out.println("Timeout: request not handled");
				}
				
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
				incrementVectorClock());
		localStore.put(msg.key, msg.value);
	}
	
	/* remove only if the msg's version is greater than the local value's
	 * version but multicast anyway if "multicast" flag is true, only the
	 * coordinator can receive the packet with removeFlag setted to true */
	private void handleRemove(RemoveMessage msg) {
		
		//TODO incrementVectorClock only iff the associated version is empty
		msg.value = (msg.multicast) ? new Versioned<byte[]>(msg.value.getValue(), 
				incrementVectorClock()) : msg.value;

		List<Versioned<byte[]>> l2 = localStore.get(msg.key);
		Versioned<byte[]> v1 = msg.value;
		Versioned<byte[]> v2 = l2 != null ? l2.get(0) : null;
		
		if ( Utils.compare(v1, v2) == 1 ) {
			VectorClock vc = (VectorClock) msg.value.getVersion();
			localStore.remove(msg.key, vc);
		}
		
		if (msg.multicast) {
			msg.multicast = false;
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
	}
	
	private void handleList(ListMessage msg) {
		for (Serializable key : localStore.list())
			msg.addKey(key);
	}
}
