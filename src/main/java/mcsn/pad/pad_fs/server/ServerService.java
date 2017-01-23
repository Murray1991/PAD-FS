package mcsn.pad.pad_fs.server;

import java.net.InetAddress;
import java.util.logging.Logger;

import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.storage.StorageService;

public class ServerService implements IService {
	
	private final static Logger LOGGER = Logger.getLogger(ServerService.class.getName());
	
	private ServerManager serverManager;
	private boolean isRunning;
	private InetAddress bindAddr;
	private int port;
	
	public ServerService(IStorageService storageService) {
		this(storageService, 8080, 0, null);
	}
	
	public ServerService(IStorageService storageService, int port) {
		this(storageService, port, 0, null);
	}
	
	public ServerService(IStorageService storageService, int port, int backlog) {
		this(storageService, port, backlog, null);
	}
	
	public ServerService(IStorageService storageService, int port, int backlog, InetAddress bindAddr) {
		this.serverManager = new ServerManager(storageService, port, backlog, bindAddr);
		this.bindAddr = bindAddr;
		this.port = port;
	}

	@Override
	public void start() {
		LOGGER.info(this + " -- start");
		this.serverManager.start();
		isRunning = true;
	}

	@Override
	public void shutdown() {
		LOGGER.info(this + " -- shutdown");
		this.serverManager.interrupt();
		isRunning = false;
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}
	
	@Override
	public String toString() {
		return "[ServerService@" + "@" + bindAddr + ":" + port + "]";
	}

}
