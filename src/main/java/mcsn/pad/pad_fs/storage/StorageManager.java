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
import mcsn.pad.pad_fs.storage.runnables.ClientHandler;
import mcsn.pad.pad_fs.storage.runnables.PullHandler;
import mcsn.pad.pad_fs.storage.runnables.PushHandler;
import mcsn.pad.pad_fs.storage.runnables.ReplyHandler;
import mcsn.pad.pad_fs.transport.Transport;

public class StorageManager extends Thread {

	private DatagramSocket udpServer;
	private InetAddress laddr;
	private int lport;
	
	private final AtomicBoolean isRunning;
	private final IStorageService storageService;
	private final ExecutorService taskPool;
	
	public StorageManager(IStorageService storageService, String host, int port) {
		this.storageService = storageService;
		
		//TODO check the "parallelism degree"
		taskPool = Executors.newFixedThreadPool(50);
		isRunning = new AtomicBoolean(true);
		
		try {
			//Make udpServer great again
			lport = port;
			laddr = InetAddress.getByName(host);
			udpServer = new DatagramSocket(port, laddr);
		} catch (SocketException | UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public int getStoragePort() {
		return lport;
	}
	
	public InetAddress getStorageAddress() {
		return laddr;
	}

	@Override
	public void run() {
		Transport transport = new Transport(udpServer, storageService);
		while (isRunning.get()) {
			try {
				SourceMessage srcMsg = transport.receive(); //UDP.srcReceive(udpServer);
				taskPool.execute(getHandler(srcMsg));
			} catch (ClassNotFoundException | IOException e) {
				System.out.println("-- Socket closed, impossible receive");
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
			return new PushHandler(srcMsg, storageService);
		case Message.PULL:
			return new PullHandler(srcMsg, storageService);
		case Message.REPLY:
			return new ReplyHandler(srcMsg, storageService);
		default:
			System.err.println("Bad srcMsg type format: " + type);
			break;
		}
		return null;
	}
	
	@Override
	public String toString() {
		return "[StorageManager@"+laddr+":"+lport;
	}
	
	@Override
	public void interrupt() {
		isRunning.set(false);
		udpServer.close();
	}
}
