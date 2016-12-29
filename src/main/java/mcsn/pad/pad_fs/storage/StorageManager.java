package mcsn.pad.pad_fs.storage;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.storage.runnables.ClientHandler;
import mcsn.pad.pad_fs.storage.runnables.PushHandler;
import mcsn.pad.pad_fs.storage.runnables.ReplyHandler;
import mcsn.pad.pad_fs.transport.UDP;

public class StorageManager extends Thread {

	private DatagramSocket udpServer;
	private InetAddress laddr;
	
	private final AtomicBoolean isRunning;
	private final IStorageService storageService;
	private final ExecutorService taskPool;
	private final LocalStore localStore;
	
	public StorageManager(IStorageService storageService, LocalStore localStore, String host, int port) {
		this.storageService = storageService;
		this.localStore		= localStore;
		
		//TODO check the "parallelism degree"
		taskPool = Executors.newFixedThreadPool(50);
		isRunning = new AtomicBoolean(true);
		
		try {
			//Make udpServer great again
			laddr = InetAddress.getByName(host);
			udpServer = new DatagramSocket(port, laddr);
		} catch (SocketException | UnknownHostException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		while (isRunning.get()) {
			try {
				SourceMessage srcMsg = UDP.srcReceive(udpServer);
				taskPool.execute(getHandler(srcMsg));
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private Runnable getHandler(SourceMessage srcMsg) {
		int type = srcMsg.msg.type;
		switch (type) {
		case Message.GET:
		case Message.PUT:
		case Message.REMOVE:
		case Message.LIST:
			return new ClientHandler(srcMsg, storageService);
		case Message.PUSH:
			return new PushHandler(srcMsg, localStore);
		case Message.REPLY:
			return new ReplyHandler(srcMsg, localStore);
		default:
			System.err.println("Bad srcMsg type format: " + type);
			break;
		}
		return null;
	}
	
	@Override
	public void interrupt() {
		isRunning.set(false);
	}
}
