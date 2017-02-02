package mcsn.pad.pad_fs.message;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

import org.junit.Assert;

import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.Versioned;

public class PushMessage extends InternalMessage {

	private static final long serialVersionUID = 5483715049287547763L;
	
	public InetAddress storageAddr;
	public int storagePort;
	
	public Vector<Serializable> keys;
	public Vector<List<Versioned<byte[]>>> values;
	
	public boolean withValue;
	
	public PushMessage(Iterable<Serializable> keys, InetAddress storageAddr, int storagePort, LocalStore localStore) {
		this.type = Message.PUSH;
		this.keys = new Vector<>();
		this.storageAddr = storageAddr;
		this.storagePort = storagePort;
		this.values = new Vector<>();
		
		Iterator<Serializable> itKey = keys.iterator();
		while (itKey.hasNext()) {
			Serializable key = itKey.next();
			List<Versioned<byte[]>> values = localStore.get(key);
			this.keys.add(key);
			/* just for practicing with java8, maybe inefficient */
			this.values.add(values
					.stream()
					.map( vv -> new Versioned<byte[]>(null, vv.getVersion()))
					.collect(Collectors.toList()));
		}
		Assert.assertTrue(this.keys.size() == this.values.size());
	}
	
	public PushMessage(Iterable<Serializable> keys, IStorageService storageService) {
		this(keys, storageService.getStorageAddress(), storageService.getStoragePort(), storageService.getLocalStore());
	}
 
	@Override
	public String toString() {
		return "boh";
	}
}
