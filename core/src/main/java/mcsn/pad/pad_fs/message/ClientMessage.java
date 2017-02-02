package mcsn.pad.pad_fs.message;

//TODO maybe better use a different class for each client-type message
public class ClientMessage extends Message {

	private static final long serialVersionUID = 6819795467160558135L;
	
	/*
	public Serializable key;
	
	public Versioned<byte[]> value;
	
	public Vector<Serializable> keys;

	public Vector<Versioned<byte[]>> values;

	public boolean removeFlag;
	
	public ClientMessage(int type, Serializable key, Versioned<byte[]> value) {
		if (!correctType(type, Message.PUT))
			throw new RuntimeException("Incorrect type for ClientMessage, PUT expected");
		this.type = type;
		this.key = key;
		this.value = value;
	}
	
	public ClientMessage(int type, Serializable key, boolean removeFlag) {
		if (!correctType(type, Message.GET, Message.REMOVE))
			throw new RuntimeException("Incorrect type for ClientMessage");
		this.type = type;
		this.key = key;
		this.value = new Versioned<byte[]>(null);
		this.removeFlag = removeFlag;	//it's significative only for REMOVE
	}
	
	public ClientMessage(int type, Serializable key) {
		if (!correctType(type, Message.GET))
			throw new RuntimeException("Incorrect type for ClientMessage");
		this.type = type;
		this.key = key;
		this.value = new Versioned<byte[]>(null);
	}
	
	public ClientMessage(int type) {
		if (!correctType(type, Message.LIST))
			throw new RuntimeException("Incorrect type for ClientMessage");
		this.type = type;
		this.value = new Versioned<byte[]>(null);
	}
	
	public void addKey(Serializable key) {
		if (keys == null && key != null)
			keys = new Vector<>();
		if (key != null)
			keys.add(key);
	}
	
	public void addValue(Versioned<byte[]> versioned) {
		if (values == null && versioned != null)
			values = new Vector<>();
		if (versioned != null)
			values.add(versioned);
	}
 
	@Override
	public String toString() {
		return "type: " + type + " ; key: " + key.toString() + " ; value: " + value.toString();
	}
	
	private boolean correctType(int type, int... types) {
		for (int expected : types) {
			if (expected == type)
				return true;
		}
		return false;
	}
	*/
}
