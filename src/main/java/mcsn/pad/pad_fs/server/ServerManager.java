package mcsn.pad.pad_fs.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import mcsn.pad.pad_fs.storage.IStorageService;

public class ServerManager extends Thread {

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
	}
	
	@Override
	public synchronized void start() {
		System.out.println("-- ServerManager: start");
		listening = true;
		super.start();
	}
	
	@Override
	public void run() {
		while (listening) {
			Socket sck = null;
			try {
				System.out.println("-- ServerManager: accept...");
				sck = serverSocket.accept();
			} catch (IOException e) {
				//e.printStackTrace();
			}
			if (sck != null)
				new ServerThread(sck, storageService).start();	//TODO maybe use executor and a fixed thread pool
		}
	}
	
	@Override
	public void interrupt() {
		System.out.println("-- ServerManager: interrupt");
		try {
			listening = false;
			serverSocket.close();
		} catch (IOException e) {
			//e.printStackTrace();
		}
		super.interrupt();
	}
}
