package mcsn.pad.pad_fs.message.client;

import java.io.Serializable;
import java.util.Vector;

import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;

public class ListMessage extends ClientMessage {

	private static final long serialVersionUID = -5578312934224027203L;

	public Vector<Serializable> keys;
	
	public ListMessage() {
		type = Message.LIST;
	}

	public void addKey(Serializable key) {
		if (keys == null && key != null)
			keys = new Vector<>();
		if (key != null)
			keys.add(key);
	}
}
