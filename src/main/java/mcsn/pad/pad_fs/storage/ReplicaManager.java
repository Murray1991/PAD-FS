package mcsn.pad.pad_fs.storage;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import mcsn.pad.pad_fs.membership.IMembershipService;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.storage.runnables.UpdateHandler;

public class ReplicaManager extends Thread {

	private final AtomicBoolean isRunning;
	private final ExecutorService taskPool;
	private final int delta;
	private final IMembershipService membershipService;
	private int storagePort;
	private InetAddress storageAddr;
	private IStorageService storageService;
	
	public ReplicaManager(IStorageService storageService, IMembershipService membershipService, int delta, String host, int storagePort) {
		this.membershipService = membershipService;
		this.storageService = storageService;
		this.delta			= delta;
		
		//TODO check the "parallelism degree"
		taskPool = Executors.newFixedThreadPool(50);
		isRunning = new AtomicBoolean(true);
		
		try {
			this.storagePort = storagePort;
			this.storageAddr = InetAddress.getByName(host);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		Random random = new Random();
		while (isRunning.get()) {
			try {
				Thread.sleep(delta);
				final List<Member> view = membershipService.getPreferenceList();
				final Member partner = view.get(random.nextInt(view.size()));
				final InetSocketAddress raddr = new InetSocketAddress(partner.host, storagePort);
				taskPool.execute(new UpdateHandler(raddr, storageAddr, storagePort, storageService));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void interrupt() {
		isRunning.set(false);
	}

}
