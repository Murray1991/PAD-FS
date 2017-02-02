package mcsn.pad.pad_fs.message.client;

import java.io.Serializable;

import voldemort.versioning.Versioned;

public class PutMessage extends RoutableClientMessage {

	private static final long serialVersionUID = 942175419311638292L;
	
	public Versioned<byte[]> value;
	
	public PutMessage(Serializable key, Versioned<byte[]> value) {
		this.key = key;
		this.value = value;
	}
	
	public PutMessage(Serializable key, byte[] value) {
		this(key, new Versioned<>(value));
	}

}
