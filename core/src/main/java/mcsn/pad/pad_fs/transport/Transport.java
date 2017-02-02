package mcsn.pad.pad_fs.transport;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.List;

import junit.framework.Assert;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.InternalMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.message.client.PutMessage;
import mcsn.pad.pad_fs.message.client.RemoveMessage;
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
			case Message.LIST:
				return;
			case Message.REMOVE:
				RemoveMessage rmsg = (RemoveMessage) msg;
				rmsg.value = new Versioned<byte[]>(
						rmsg.value.getValue(), vc);
				return;
			case Message.PUT:
				PutMessage pmsg = (PutMessage) msg;
				pmsg.value = new Versioned<byte[]>(
						pmsg.value.getValue(), vc);
				return;
			case Message.PUSH:
			case Message.PULL:
			case Message.REPLY:
				/*in the case of anti-entropy message just copy the global vc */
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
			case Message.LIST:
				return;
			case Message.REMOVE:
				storageService.mergeAndIncrementVectorClock( (VectorClock)
						((RemoveMessage) msg).value.getVersion() );
				return;
			case Message.PUT:
				storageService.mergeAndIncrementVectorClock( (VectorClock)
						((PutMessage) msg).value.getVersion() );
				return;
			case Message.PUSH:
			case Message.PULL:
			case Message.REPLY:
				//Here merge & increment is necessary for possible partition network
				storageService.mergeAndIncrementVectorClock( 
						((InternalMessage) msg).vectorClock);
				return;
			default:
				System.err.println("Bad srcMsg type format: " + type);
				break;
		}
	}

	public void multicast(ClientMessage msg, List<Member> members) {
		int count = 0;
		for (Member member : members) {
			try {
				send(msg, new InetSocketAddress(member.host, storageService.getStoragePort()));
			} catch (IOException e) {
				count++;
			} 
		}
		Assert.assertTrue(count == 0);
		for (int i = 0; i < members.size(); i++) {
			try {
				receive();
			} catch (IOException | ClassNotFoundException e) {
			}
		}
	}
	
}
