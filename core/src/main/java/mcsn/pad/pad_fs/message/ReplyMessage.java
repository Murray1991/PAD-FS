package mcsn.pad.pad_fs.message;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.Versioned;

public class ReplyMessage extends InternalMessage {

	private static final long serialVersionUID = 1743885100223042523L;
	
	public Vector<Serializable> keys;
	
	public Vector<List<Versioned<byte[]>>> values;
	
	public ReplyMessage(Iterable<Serializable> keys, LocalStore localStore) {
		this.type = Message.REPLY;
		this.keys = new Vector<>();
		this.values = new Vector<>();
		Iterator<Serializable> itKey = keys.iterator();
		while (itKey.hasNext()) {
			Serializable key = itKey.next();
			List<Versioned<byte[]>> values = localStore.get(key);
			this.keys.add(key);
			this.values.add(values);
		}
	}
 
	public ReplyMessage(Map<Serializable, List<Versioned<byte[]>>> map) {
		this.type = Message.REPLY;
		this.keys = new Vector<>();
		this.values = new Vector<>();
		for (Entry<Serializable, List<Versioned<byte[]>>> e : map.entrySet()) {
			this.keys.add(e.getKey());
			this.values.add(e.getValue());
		}
	}

	@Override
	public String toString() {
		return "type: " + type + " ; # keys: " + keys.size() + " ; # values: " + values.size();
	}
}
