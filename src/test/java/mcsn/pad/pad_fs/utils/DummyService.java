package mcsn.pad.pad_fs.utils;

import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.storage.IStorageService;

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

}
