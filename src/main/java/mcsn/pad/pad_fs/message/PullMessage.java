package mcsn.pad.pad_fs.message;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Vector;

public class PullMessage extends InternalMessage {

	private static final long serialVersionUID = 7196082854063916513L;

	public InetAddress storageAddr;
	public int storagePort;
	
	public Vector<Serializable> keys;
	
	public PullMessage(Iterable<Serializable> keys, InetAddress storageAddr, int storagePort) {
		this.type = Message.PULL;
		this.keys = new Vector<>();
		this.storageAddr = storageAddr;
		this.storagePort = storagePort;
		Iterator<Serializable> itKey = keys.iterator();
		while (itKey.hasNext()) {
			Serializable key = itKey.next();
			this.keys.add(key);
		}
	}
	
	@Override
	public String toString() {
		return "boh";
	}
}
