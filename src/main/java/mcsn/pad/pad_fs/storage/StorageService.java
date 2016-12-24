package mcsn.pad.pad_fs.storage;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.logging.Logger;

import mcsn.pad.pad_fs.membership.IMembershipService;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.transport.UDP;

public class StorageService implements IStorageService {
	
	private final static Logger LOGGER = Logger.getLogger(StorageService.class.getName());
	
	private boolean isRunning;
	private StorageManager storageManager;
	private IMembershipService membershipService;
	private LocalStore localStore;
	private final int storageManagerPort = 3000; //TODO

	public StorageService(IMembershipService membershipService, LocalStore localStore) {
		this.membershipService = membershipService;
		this.localStore = localStore;
		String host = membershipService.getMyself().host;
		storageManager = new StorageManager(this, host, storageManagerPort);
	}

	@Override
	public void start() {
		LOGGER.info(this + " -- start");
		isRunning = true;
		storageManager.start();
	}
	
	@Override
	public void shutdown() {
		LOGGER.info(this + " -- shutdown");
		isRunning = false;
		localStore.close(); //TODO check
		storageManager.interrupt();
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}

	//TODO maybe better name is "HandleRequest"
	@Override
	public Message deliverMessage(Message msg) {
		Member coordinator = membershipService.getCoordinator((String) msg.key); //TODO check
		Member myself = membershipService.getMyself();
		Message rcvMsg = null;
		
		if (coordinator.equals(myself)) {
			//System.out.println(this + " -- coordinator for " + msg.key);
			resolveMessage(msg);
			rcvMsg = msg;
		} else {
			//System.out.println(this + " -- send " + msg.key + " to the coordinator " + coordinator.host + ":" + storageManagerPort);
			try {
				DatagramSocket socket = new DatagramSocket();
				UDP.send(msg, socket, new InetSocketAddress(coordinator.host, storageManagerPort)); 
				rcvMsg = UDP.receive(socket); 
				//TODO check if rcvMsg is correct
				//TODO what if I don't receive the message? Do something with timeouts?
				socket.close();
			} catch (SocketException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return rcvMsg;
	}
	
	private void resolveMessage(Message msg) {
		msg.status = Message.UNKNOWN;
		switch (msg.type) {
			case Message.GET:
				msg.value = localStore.get(msg.key);
				break;
			case Message.PUT:
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
	
	public String toString() {
		return "[StorageService@"+ this.membershipService.getMyself().id + "]";
	}
}
