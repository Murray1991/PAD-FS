package mcsn.pad.pad_fs.storage;

import java.io.IOException;
import java.lang.Thread.State;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

import org.apache.log4j.Logger;
import org.junit.Assert;

import mcsn.pad.pad_fs.membership.IMembershipService;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.client.RemoveMessage;
import mcsn.pad.pad_fs.message.client.RoutableClientMessage;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.transport.Transport;
import voldemort.versioning.VectorClock;

public class StorageService implements IStorageService {
	
	private final static Logger logger = Logger.getLogger(StorageService.class);
	
	private boolean isRunning = false;
	private StorageManager storageManager;
	private ReplicaManager replicaManager;
	private IMembershipService membershipService;
	private LocalStore localStore;
	
	private int storageManagerPort; 

	private VectorClock vectorClock;

	private int ID;

	private String host;

	private int updateInterval;

	private ResolveHelper resolveHelper;

	public StorageService(IMembershipService membershipService, int port, int interval, LocalStore localStore) {
		this.storageManagerPort = port;
		this.updateInterval = interval;
		this.membershipService = membershipService;
		this.localStore = localStore;
		this.host = membershipService.getMyself().host;
		this.storageManager = new StorageManager(
				this, host, storageManagerPort);
		this.replicaManager = new ReplicaManager(
				this, membershipService, 1000, host, storageManagerPort);
		
		//TODO Maybe better way to get an ID
		this.ID = Math.abs(host.hashCode()) % Short.MAX_VALUE;
		
		//for versioning
		this.vectorClock = new VectorClock();
		if (localStore.size() == 0) {
			this.vectorClock.incrementVersion(this.ID, System.currentTimeMillis());
		} else { 
			/* it merges all the versions present in the local store */
			updateVectorClock();
		}
		
		//resolve the client message and reads/writes from/into the DB
		this.resolveHelper = new ResolveHelper(this, membershipService, localStore);
	}

	/** default port = 3000 constructor */
	public StorageService(IMembershipService membershipService, LocalStore localStore) {
		this(membershipService, 3000, 1000, localStore);
	}

	@Override
	public void start() {
		logger.info(this + " -- start");
		State stateS = storageManager.getState();
		State stateR = replicaManager.getState();
		if (! stateS.equals(State.NEW) ) {
			storageManager = new StorageManager(this, host, storageManagerPort);
		}
		if (! stateR.equals(State.NEW)) {
			replicaManager = new ReplicaManager(this, membershipService, updateInterval, host, storageManagerPort);
		}
		if (!isRunning) {
			isRunning = true;
			storageManager.start();
			replicaManager.start();
		}
	}
	
	@Override
	public void shutdown() {
		logger.info(this + " -- shutdown");
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

	public ClientMessage deliverMessage(ClientMessage msg) {
		ClientMessage rcvMsg = null;
		boolean success = false;
		int attempts = 0;
		
		/* at least two attempts in case the coordinator is down */
		while (attempts++ < 2 && !success) {
			Member coordinator = toRoute(msg);
			if (coordinator == null) {
				/* the actual node is the coordinator or the message is not routable
				 * or the message is a remove message with multicast field set to false*/
				resolveHelper.resolveMessage(msg);
				rcvMsg = msg;
				success = true;
			} else {
				/* routable message but the actual node is not the coordinator */
				DatagramSocket socket = null;
				try {
					socket = new DatagramSocket();
					Transport transport = new Transport(socket, this);
					transport.send(msg, new InetSocketAddress(coordinator.host, storageManagerPort));
					rcvMsg = (ClientMessage) transport.receive(5000).msg;
					success = true;
				} catch (SocketTimeoutException e) {
					logger.trace(e, e.getCause());
				} catch (IOException | ClassNotFoundException e) {
					logger.trace(e, e.getCause());
				} finally {
					if (socket != null)
						socket.close();
				}
			}
		}
		
		if (!success) {
			rcvMsg = msg;
			rcvMsg.status = Message.ERROR;
		}
		
		return rcvMsg;
	}
	
	@Override
	public String toString() {
		return "[StorageService@"+ this.membershipService.getMyself().id + "@" + getID() + "]";
	}
	
	private Member toRoute(ClientMessage msg) {
		Member myself = membershipService.getMyself();
		Member coordinator = null;
		/* check if it's a remove message and its multicast property */
		boolean isRemove = false;
		boolean multicast = false;
		if ( msg instanceof RemoveMessage ) {
			isRemove = true;
			multicast = ((RemoveMessage) msg).multicast;
		}
		/* compute the coordinator only if it's a routable message 
		 * or the multicast field of remove message is set to false */
		if ( msg instanceof RoutableClientMessage && !(isRemove && !multicast)) {
			String key = (String) ((RoutableClientMessage) msg).key;
			coordinator = membershipService.getCoordinator(key);
			Assert.assertNotNull(coordinator);
		}
		/* return the coordinator only if the message needs to be routed */
		return coordinator != null && coordinator.equals(myself) ?
				null : coordinator;		
	}
	
	private void updateVectorClock() {
		localStore.list().forEach( key -> {
			localStore.get(key).forEach( value -> {
				VectorClock clock = (VectorClock) value.getVersion();
				vectorClock = vectorClock.merge(clock );
			});
		});
	}
}
