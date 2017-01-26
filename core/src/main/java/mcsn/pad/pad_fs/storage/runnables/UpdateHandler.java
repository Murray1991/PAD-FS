package mcsn.pad.pad_fs.storage.runnables;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;

import mcsn.pad.pad_fs.message.PushMessage;
import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.transport.Transport;

/**
 * 
 * @author Leonardo Gazzarri
 *
 * It sends to a partner a sequence of Push messages representing the
 * current view of the running storage service.
 */
public class UpdateHandler implements Runnable {
	
	final private InetSocketAddress partner;
	final private LocalStore localStore;
	private InetAddress storageAddress;
	private int storagePort;
	private IStorageService storageService;

	public UpdateHandler(InetSocketAddress partner, InetAddress storageAddress, int storagePort, IStorageService storageService) {
		this.partner = partner;
		this.storageAddress = storageAddress;
		this.storagePort = storagePort;
		this.storageService = storageService;
		this.localStore = storageService.getLocalStore();
	}

	@Override
	public void run() {
		Iterable<Serializable> keys = localStore.list();
		if (keys.iterator().hasNext()) {
			Assert.assertTrue(""+localStore.size(), localStore.size() != 0);
			Iterator<Serializable> itKey = keys.iterator();
			while (itKey.hasNext()) {
				//TODO add keys to pieces list until max possible packet size has been reached
				final List<Serializable> pieces = new ArrayList<Serializable>(100);
				while (itKey.hasNext() && pieces.size() < 100)
					pieces.add(itKey.next());
				try {
					Transport transport = new Transport(storageService);
					transport.send(new PushMessage(pieces, storageAddress, storagePort, localStore), partner);
				} catch (IOException e) {
					System.out.println("-- Socket closed, impossible send");
				}
			}
		}
	}

}
