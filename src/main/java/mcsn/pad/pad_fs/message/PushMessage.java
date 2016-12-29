package mcsn.pad.pad_fs.message;

import java.io.Serializable;

import voldemort.versioning.Versioned;

public class PushMessage extends Message {

	private static final long serialVersionUID = 5483715049287547763L;

	public Serializable key;

	public Versioned<byte[]> value;
	
	public PushMessage(Serializable key, Versioned<byte[]> value) {
		this.type = Message.PUSH;
		this.key = key;
		this.value = value;
	}
 
	@Override
	public String toString() {
		return "type: " + type + " ; key: " + key.toString() + " ; value: " + value.toString();
	}
}
