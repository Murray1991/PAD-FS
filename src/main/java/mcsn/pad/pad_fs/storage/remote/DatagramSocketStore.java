package mcsn.pad.pad_fs.storage.remote;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Map;

import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.transport.UDP;
import voldemort.versioning.Versioned;

public class DatagramSocketStore extends RemoteStore {
	
	private InetSocketAddress addr;

    public DatagramSocketStore(String ipAddress, int port) throws SocketException, UnknownHostException {
		this.addr = new InetSocketAddress(InetAddress.getByName(ipAddress), port);
	}
    
    public DatagramSocketStore(Member member, int port) throws UnknownHostException {
    	this.addr = new InetSocketAddress(InetAddress.getByName(member.host), port);
    }
	
	public DatagramSocketStore(InetSocketAddress addr) {
		this.addr = addr;
	}

	@Override
	public void put(Serializable key, Versioned<byte[]> value) {
		try {
			UDP.send(new Message(PUT, key, value), this.addr);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public Versioned<byte[]> get(Serializable key) {
		try {
			UDP.send(new Message(GET, key, null), this.addr);
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
				UDP.send(new Message(GET, key, null), this.addr);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
}
