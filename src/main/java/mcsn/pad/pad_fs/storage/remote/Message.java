package mcsn.pad.pad_fs.storage.remote;

import java.io.Serializable;

import voldemort.versioning.Versioned;

public class Message implements Serializable {

	private static final long serialVersionUID = 319471187021291131L;

	public int type;

	public Serializable key;

	public Versioned<byte[]> value;
	
	public Message(int type, Serializable key, Versioned<byte[]> value) {
		this.type = type;
		this.key = key;
		this.value = value;
	}
 
}
