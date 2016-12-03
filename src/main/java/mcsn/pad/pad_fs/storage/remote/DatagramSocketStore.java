package mcsn.pad.pad_fs.storage.remote;

import java.io.IOException;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;

import mcsn.pad.pad_fs.common.Utils;
import voldemort.versioning.Versioned;

public class DatagramSocketStore extends RemoteStore {
	
	private InetSocketAddress addr;

    public DatagramSocketStore(String ipAddress, int port) throws SocketException, UnknownHostException {
		this.addr = new InetSocketAddress(InetAddress.getByName(ipAddress), port);
	}
	
	public DatagramSocketStore(InetSocketAddress addr) {
		this.addr = addr;
	}

	@Override
	public void put(Serializable key, Versioned<byte[]> value) {
		try {
			send(new Message(PUT, key, value));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public Versioned<byte[]> get(Serializable key) {
		try {
			send(new Message(GET, key, null));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public Map<Serializable, Versioned<byte[]>> list(Iterable<Serializable> keys) {
		//TODO send a LIST message with all the keys -> reduce traffic
		for (Serializable key : keys) {
			try {
				send(new Message(GET, key, null));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	private void send(Message msg) throws IOException {
	
	    byte[] data = Utils.convertToBytes(msg);
	    byte[] length = new byte[4];
	    // int -> byte[]
	    for (int i = 0; i < 4; ++i) {
	    	int shift = i << 3; // i * 8
	    	length[3-i] = (byte)((data.length & (0xff << shift)) >>> shift);
	    }
	    
		ByteBuffer byteBuffer = ByteBuffer.allocate(4 + data.length);
		byteBuffer.put(length);
		byteBuffer.put(data);
		byte[] buf = byteBuffer.array();

		//TODO bind to a local address and a port <--- probably not essential
		//TODO what if data.length is bigger than the maximum pkt size?
		DatagramSocket socket = new DatagramSocket();
		DatagramPacket pkt = new DatagramPacket(buf, buf.length, addr);
		socket.send(pkt);
		socket.close();	
	}
}
