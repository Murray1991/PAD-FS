package mcsn.pad.pad_fs.message;

import java.io.Serializable;

import voldemort.versioning.Versioned;

public class ClientMessage extends Message {

	private static final long serialVersionUID = 6819795467160558135L;
	
	public Serializable key;

	public Versioned<byte[]> value;
	
	public ClientMessage(int type, Serializable key, Versioned<byte[]> value) {
		if (!correctType(type))
			throw new RuntimeException("Incorrect type for ClientMessage");
		this.type = type;
		this.key = key;
		this.value = value;
	}
	
	private boolean correctType(int type) {
		return type == Message.GET ||
				type == Message.PUT ||
				type == Message.LIST ||
				type == Message.REMOVE;
	}
 
	@Override
	public String toString() {
		return "type: " + type + " ; key: " + key.toString() + " ; value: " + value.toString();
	}
}
