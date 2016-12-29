package mcsn.pad.pad_fs.message;

import java.io.Serializable;

import voldemort.versioning.Versioned;

public class ReplyMessage extends Message {

	private static final long serialVersionUID = 1743885100223042523L;

	public Serializable key;

	public Versioned<byte[]> value;
	
	public ReplyMessage(Serializable key, Versioned<byte[]> value) {
		this.type = Message.REPLY;
		this.key = key;
		this.value = value;
	}
 
	@Override
	public String toString() {
		return "type: " + type + " ; key: " + key.toString() + " ; value: " + value.toString();
	}
}
