package mcsn.pad.pad_fs.storage;

import java.net.InetAddress;

import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.VectorClock;

public interface IStorageService extends IService {
	
	public ClientMessage deliverMessage(ClientMessage msg);
	
	public int getID();
	
	/* synchronized method */
	public VectorClock getVectorClock();
	
	/* synchronize method */
	public VectorClock incrementVectorClock();
	
	/* synchronize method */
	public VectorClock mergeAndIncrementVectorClock(VectorClock vc);
	
	/* synchronized method */
	public VectorClock mergeVectorClock(VectorClock vc);
	
	public LocalStore getLocalStore();
	
	public int getStoragePort();
	
	public InetAddress getStorageAddress();
}
