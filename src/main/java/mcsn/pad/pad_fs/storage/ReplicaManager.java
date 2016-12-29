package mcsn.pad.pad_fs.storage;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import mcsn.pad.pad_fs.membership.IMembershipService;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.storage.runnables.UpdateHandler;

public class ReplicaManager extends Thread {

	private final AtomicBoolean isRunning;
	private final ExecutorService taskPool;
	private final LocalStore localStore;
	private final int delta;
	private final IMembershipService membershipService;
	private int storagePort;
	
	public ReplicaManager(IMembershipService membershipService, LocalStore localStore, int delta, String host, int storagePort) {
		this.membershipService = membershipService;
		this.storagePort	= storagePort;
		this.localStore		= localStore;
		this.delta			= delta;
		
		//TODO check the "parallelism degree"
		taskPool = Executors.newFixedThreadPool(50);
		isRunning = new AtomicBoolean(true);
	}

	@Override
	public void run() {
		Random random = new Random();
		while (isRunning.get()) {
			try {
				Thread.sleep(delta);
				final List<Member> view = membershipService.getPreferenceList();
				final int index = random.nextInt(view.size());
				
				final Member partner = view.get(index);
				final InetSocketAddress raddr = new InetSocketAddress(partner.host, storagePort);
				
				taskPool.execute(new UpdateHandler(raddr, localStore));
				
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
