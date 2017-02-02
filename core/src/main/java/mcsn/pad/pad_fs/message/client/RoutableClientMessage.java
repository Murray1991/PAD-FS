package mcsn.pad.pad_fs.message.client;

import java.io.Serializable;

import mcsn.pad.pad_fs.message.ClientMessage;

public class RoutableClientMessage extends ClientMessage {

	private static final long serialVersionUID = 3381631701578362086L;
	
	public Serializable key;
}
