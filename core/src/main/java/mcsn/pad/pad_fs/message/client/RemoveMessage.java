package mcsn.pad.pad_fs.message.client;

import java.io.Serializable;

import mcsn.pad.pad_fs.message.Message;
import voldemort.versioning.Versioned;

public class RemoveMessage extends RoutableClientMessage {

	private static final long serialVersionUID = -2123518740045932815L;
	
	public boolean multicast;

	public Versioned<byte[]> value;
	
	public RemoveMessage(Serializable key) {
		this.type = Message.REMOVE;
		this.key = key;
		this.value = new Versioned<byte[]>(null);
		this.multicast = true;
	}
	
}
