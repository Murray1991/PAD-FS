package mcsn.pad.pad_fs.server;

import java.net.InetAddress;

import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.message.Message;

public class ServerService implements IService {
	
	private ServerManager serverManager;
	private boolean isRunning;
	
	public ServerService(IService storageService) {
		this(storageService, 8080, 0, null);
	}
	
	public ServerService(IService storageService, int port) {
		this(storageService, port, 0, null);
	}
	
	public ServerService(IService storageService, int port, int backlog) {
		this(storageService, port, backlog, null);
	}
	
	public ServerService(IService storageService, int port, int backlog, InetAddress bindAddr) {
		this.serverManager = new ServerManager(storageService, port, backlog, bindAddr);
	}

	@Override
	public void start() {
		this.serverManager.start();
		isRunning = true;
	}

	@Override
	public void shutdown() {
		this.serverManager.interrupt();
		isRunning = false;
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}

	@Override
	public Message deliverMessage(Message msg) {
		return null;
	}
}
