package mcsn.pad.pad_fs.utils;

import java.net.InetAddress;

import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.VectorClock;

public class DummyService implements IStorageService {

	@Override
	public void start() {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public boolean isRunning() {
		return true;
	}

	@Override
	public ClientMessage deliverMessage(ClientMessage msg) {
		try { 
			Thread.sleep(500);
		} catch (InterruptedException e) {
		}
		return TestUtils.randomMessage(msg);
	}

	@Override
	public VectorClock getVectorClock() {
		return null;
	}

	@Override
	public LocalStore getLocalStore() {
		return null;
	}

	@Override
	public int getStoragePort() {
		return 0;
	}

	@Override
	public InetAddress getStorageAddress() {
		return null;
	}

	@Override
	public VectorClock mergeVectorClock(VectorClock vc) {
		return null;
	}

	@Override
	public int getID() {
		return 0;
	}

	@Override
	public VectorClock incrementVectorClock() {
		return null;
	}

	@Override
	public VectorClock mergeAndIncrementVectorClock(VectorClock vc) {
		return null;
	}

}
