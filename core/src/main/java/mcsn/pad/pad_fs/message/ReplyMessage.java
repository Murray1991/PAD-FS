package mcsn.pad.pad_fs.message;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;

import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.Versioned;

public class ReplyMessage extends InternalMessage {

	private static final long serialVersionUID = 1743885100223042523L;
	
	public Vector<Serializable> keys;
	
	public Vector<Versioned<byte[]>> values;
	
	public ReplyMessage(Iterable<Serializable> keys, Iterable<Versioned<byte[]>> values) {
		this.type = Message.REPLY;
		this.keys = new Vector<>();
		this.values = new Vector<>();
		Iterator<Serializable> itKey = keys.iterator();
		Iterator<Versioned<byte[]>> itValue = values.iterator();
		while (itKey.hasNext()) {
			Serializable key = itKey.next();
			Versioned<byte[]> value = itValue.next();
			this.keys.add(key);
			this.values.add(value);
		}
	}
	
	public ReplyMessage(Iterable<Serializable> keys, LocalStore localStore) {
		this.type = Message.REPLY;
		this.keys = new Vector<>();
		this.values = new Vector<>();
		Iterator<Serializable> itKey = keys.iterator();
		while (itKey.hasNext()) {
			Serializable key = itKey.next();
			Versioned<byte[]> value = localStore.get(key);
			this.keys.add(key);
			this.values.add(value);
		}
	}
 
	@Override
	public String toString() {
		return "type: " + type + " ; # keys: " + keys.size() + " ; # values: " + values.size();
	}
}
