package mcsn.pad.pad_fs.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import mcsn.pad.pad_fs.storage.IStorageService;

public class ServerManager extends Thread {

	private final ExecutorService taskPool;
	private IStorageService storageService;
	private ServerSocket serverSocket;
	private boolean listening;

	public ServerManager(IStorageService storageService, int port, int backlog, InetAddress bindAddr ) {
		try {
			this.storageService = storageService;
			this.serverSocket = new ServerSocket(port, backlog, bindAddr);
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.taskPool = Executors.newCachedThreadPool();
	}
	
	@Override
	public synchronized void start() {
		listening = true;
		super.start();
	}
	
	@Override
	public void run() {
		while (listening) {
			Socket sck = null;
			try {
				sck = serverSocket.accept();
				taskPool.submit(new ServerThread(sck, storageService));
			} catch (IOException e) {
				System.out.println("Socket closed in Server Manager");
			}
		}
	}
	
	@Override
	public void interrupt() {
		try {
			listening = false;
			serverSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		super.interrupt();
	}
}
