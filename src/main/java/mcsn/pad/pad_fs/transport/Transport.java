package mcsn.pad.pad_fs.transport;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;

import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.InternalMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.storage.IStorageService;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class Transport {
	
	private DatagramSocket udpServer;
	private IStorageService storageService;

	public Transport(IStorageService storageService) {
		this.udpServer = null;
		this.storageService = storageService;
	}
	
	public Transport(DatagramSocket udpServer, IStorageService storageService) {
		this.udpServer = udpServer;
		this.storageService = storageService;
	}

	public SourceMessage receive() throws ClassNotFoundException, IOException {
		SourceMessage srcMsg = UDP.srcReceive(udpServer);
		onReceive(srcMsg.msg, srcMsg.msg.type);
		return srcMsg;
	}
	
	public void send(Message msg, InetSocketAddress raddr) throws IOException {
		if (msg != null && raddr != null && udpServer == null) {
			onSend(msg, msg.type);
			UDP.send(msg, raddr);
		}
		
		if (msg != null && raddr != null && udpServer != null) {
			onSend(msg, msg.type);
			UDP.send(msg, udpServer, raddr);
		}
	}
	
	private void onSend(Message msg, int type) {
		VectorClock vc = storageService.incrementVectorClock();
		switch (type) {
		case Message.GET:
		case Message.PUT:
		case Message.REMOVE:
		case Message.LIST:
			ClientMessage cmsg = (ClientMessage) msg;
			cmsg.value = new Versioned<byte[]>(cmsg.value.getValue(), vc);
			return;
		case Message.PUSH:
		case Message.PULL:
		case Message.REPLY:
			InternalMessage imsg = (InternalMessage) msg;
			imsg.vectorClock = vc;
			return;
		default:
			System.err.println("Bad srcMsg type format: " + type);
			break;
		}
	}
	
	private void onReceive(Message msg, int type) {
		switch (type) {
		case Message.GET:
		case Message.PUT:
		case Message.REMOVE:
		case Message.LIST:
			storageService.mergeAndIncrementVectorClock( (VectorClock)
					((ClientMessage) msg).value.getVersion() );
			return;
		case Message.PUSH:
		case Message.PULL:
		case Message.REPLY:
			storageService.mergeAndIncrementVectorClock( 
					((InternalMessage) msg).vectorClock);
			return;
		default:
			System.err.println("Bad srcMsg type format: " + type);
			break;
		}
	}
	
}
