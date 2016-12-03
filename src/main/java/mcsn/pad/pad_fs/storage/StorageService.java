package mcsn.pad.pad_fs.storage;

import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.membership.MembershipService;

public class StorageService implements IService {
	
	private boolean isRunning;
	private StorageManager storageManager;
	
	public StorageService(MembershipService membershipService) {
		storageManager = new StorageManager(membershipService);
	}

	@Override
	public void start() {
		isRunning = true;
		storageManager.start();
	}
	
	@Override
	public void shutdown() {
		isRunning = false;
		storageManager.interrupt();
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}

}
