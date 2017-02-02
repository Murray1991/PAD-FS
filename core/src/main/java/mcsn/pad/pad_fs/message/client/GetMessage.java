package mcsn.pad.pad_fs.message.client;

import java.io.Serializable;
import java.util.Vector;

import mcsn.pad.pad_fs.message.Message;
import voldemort.versioning.Versioned;

public class GetMessage extends RoutableClientMessage {

	private static final long serialVersionUID = 4799037807381986722L;
	
	public Vector<Versioned<byte[]>> values;
	
	public GetMessage(Serializable key) {
		this.type = Message.GET;
		this.key = key;
	}
	
}
