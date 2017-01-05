package mcsn.pad.pad_fs.utils;

import java.net.InetAddress;

import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.VectorClock;

public class DummyService implements IStorageService {

	@Override
	public void start() {
		// TODO Auto-generated method stub
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
	}

	@Override
	public boolean isRunning() {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LocalStore getLocalStore() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getStoragePort() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public InetAddress getStorageAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VectorClock mergeVectorClock(VectorClock vc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getID() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public VectorClock incrementVectorClock() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VectorClock mergeAndIncrementVectorClock(VectorClock vc) {
		// TODO Auto-generated method stub
		return null;
	}

}
