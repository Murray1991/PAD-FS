package mcsn.pad.pad_fs.storage.runnables;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;

import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.transport.UDP;

public class ClientHandler implements Runnable {
	
	final private SourceMessage srcMsg;
	final private IStorageService storageService;
	final private InetSocketAddress raddr;

	public ClientHandler(SourceMessage srcMsg, IStorageService storageService) {
		this.srcMsg = srcMsg;
		this.storageService = storageService;
		this.raddr = new InetSocketAddress(srcMsg.addr, srcMsg.port);
	}

	@Override
	public void run() {
		try {
			ClientMessage msg = storageService.deliverMessage( (ClientMessage) srcMsg.msg);
			DatagramSocket socket = new DatagramSocket();
			UDP.send(msg, socket, raddr);
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
