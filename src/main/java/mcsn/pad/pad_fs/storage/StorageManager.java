package mcsn.pad.pad_fs.storage;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.transport.UDP;

public class StorageManager extends Thread {

	private DatagramSocket udpServer;
	private InetAddress laddr;
	
	private final AtomicBoolean isRunning;
	private final IStorageService storageService;
	private final ExecutorService taskPool;
	
	public StorageManager(IStorageService storageService, String host, int port) {
		this.storageService = storageService;
		taskPool = Executors.newCachedThreadPool();
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
				final SourceMessage srcMsg = UDP.srcReceive(udpServer);
				final InetSocketAddress raddr = new InetSocketAddress(srcMsg.addr, srcMsg.port);
				taskPool.execute(new Runnable() {
					@Override
					public void run() {
						try {
							Message msg = storageService.deliverMessage(srcMsg.msg);
							DatagramSocket socket = new DatagramSocket();
							UDP.send(msg, socket, raddr);
							socket.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void interrupt() {
		isRunning.set(false);
	}
}
