package mcsn.pad.pad_fs.message;

import java.io.Serializable;

import voldemort.versioning.Versioned;

public class Message implements Serializable {

	private static final long serialVersionUID = 319471187021291131L;
	
	public static final int MAX_PACKET_SIZE = 102400;
	
	public static final int PUT 	= 0;
	public static final int GET 	= 1;
	public static final int LIST 	= 2;
	public static final int REMOVE 	= 3;
	public static final int SUCCESS = 4;
	public static final int ERROR 	= 5;
	public static final int UNKNOWN = 6;
	public static final int OK 		= 4;
	public static final int KO 		= 5;

	public int type;
	
	public int status;

	public Serializable key;

	public Versioned<byte[]> value;
	
	public Message(int type, Serializable key, Versioned<byte[]> value) {
		this.type = type;
		this.key = key;
		this.value = value;
	}
 
	@Override
	public String toString() {
		return "type: " + type + " ; key: " + key.toString() + " ; value: " + value.toString();
	}
}
