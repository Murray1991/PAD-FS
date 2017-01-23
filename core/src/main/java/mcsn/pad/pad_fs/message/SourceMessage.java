package mcsn.pad.pad_fs.message;

import java.net.DatagramPacket;
import java.net.InetAddress;

public class SourceMessage {
	public Message msg;
	public InetAddress addr;
	public int port;
	
	public SourceMessage(Message msg, DatagramPacket p) {
		this.msg = msg;
		this.addr = p.getAddress();
		this.port = p.getPort();
	}
}
