package mcsn.pad.pad_fs.storage.runnables;

import java.io.IOException;
import java.net.InetSocketAddress;

import mcsn.pad.pad_fs.message.PullMessage;
import mcsn.pad.pad_fs.message.ReplyMessage;
import mcsn.pad.pad_fs.message.SourceMessage;
import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.transport.Transport;

/**
 * 
 * @author Leonardo Gazzarri
 *
 * It handles a Pull message: the receiver of this message send back
 * to the sender a collection of <Key, Value> messages where the keys
 * are exactly the requested ones in the Pull message.
 */
public class PullHandler implements Runnable {
	
	final private PullMessage pullMsg;
	final private LocalStore localStore;
	final private InetSocketAddress raddr;
	final private IStorageService storageService;
	
	public PullHandler(SourceMessage msg, IStorageService storageService) {
		this.pullMsg = (PullMessage) msg.msg;
		this.localStore = storageService.getLocalStore();
		this.storageService = storageService;
		this.raddr = new InetSocketAddress(pullMsg.storageAddr, pullMsg.storagePort);
	}

	@Override
	public void run() {
		ReplyMessage msg = new ReplyMessage(pullMsg.keys, localStore);
		try {
			Transport transport = new Transport(storageService);
			transport.send(msg, raddr);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
