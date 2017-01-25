package mcsn.pad.pad_fs_cli;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import voldemort.versioning.Versioned;

public class Client {

	private InetSocketAddress raddr;

	public Client(InetSocketAddress raddr) {
		this.raddr = raddr;
	}

	public ClientMessage send(ClientMessage sendMsg) throws ClassNotFoundException, IOException {
		Socket sck = new Socket(raddr.getAddress(), raddr.getPort());
		ClientMessage rcvMsg = request(sendMsg, sck);
		sck.close();
		return rcvMsg;
	}
	
	public ClientMessage send(int type, String[] values) throws ClassNotFoundException, IOException {
		Socket sck = new Socket(raddr.getAddress(), raddr.getPort());
		ClientMessage sendMsg = null;
		if (type == Message.LIST) {
			sendMsg = new ClientMessage(type);
		} else if (type == Message.REMOVE || type == Message.GET) {
			sendMsg = new ClientMessage(type, values[0], true);
		} else if (type == Message.PUT) {
			Versioned<byte[]> value = values.length > 1 ? 
					getVersionedFromString(values[1]) : getVersionedFromFile(values[0]);
			sendMsg = new ClientMessage(type, values[0], value);
		}
		ClientMessage rcvMsg = request(sendMsg, sck);
		sck.close();
		return rcvMsg;
	}
	
	private ClientMessage request(Message sendMsg, Socket sck) throws IOException, ClassNotFoundException {
		ObjectOutputStream oos = new ObjectOutputStream(sck.getOutputStream());
		oos.writeObject(sendMsg);
		ObjectInputStream ois = new ObjectInputStream(sck.getInputStream());
		ClientMessage msg = (ClientMessage) ois.readObject();
		ois.close();
		oos.close();
		return msg;
	}
	
	private Versioned<byte[]> getVersionedFromFile(String path) throws IOException {
		byte[] data = Files.readAllBytes( Paths.get(path) );
		return new Versioned<byte[]>(data);
	}
	
	private Versioned<byte[]> getVersionedFromString(String value) {
		byte[] data = value.getBytes();
		return new Versioned<byte[]>(data);
	}

}
