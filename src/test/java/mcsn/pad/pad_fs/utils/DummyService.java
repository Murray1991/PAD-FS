package mcsn.pad.pad_fs.utils;

import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.message.Message;

public class DummyService implements IService {

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
	public Message deliverMessage(Message msg) {
		try { 
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
		return TestUtils.randomMessage(msg);
	}

}
