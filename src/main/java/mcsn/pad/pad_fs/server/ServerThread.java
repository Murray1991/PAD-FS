package mcsn.pad.pad_fs.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.storage.IStorageService;

public class ServerThread extends Thread {

	private Socket socket;
	private IStorageService storageService;
	
	public ServerThread(Socket socket, IStorageService storageService) {
		this.socket = socket;
		this.storageService = storageService;
	}

	@Override
	public void run() {
		System.out.println("-- Thread "  + this.getId() + ": started");
		ObjectInputStream ois = null;
		ObjectOutputStream oos = null;
		Message msg = null;
		try {
			ois = new ObjectInputStream(socket.getInputStream()); //TODO use Future and timeout
			msg = (Message) ois.readObject();
			if (msg != null) {
				msg = storageService.deliverMessage(msg);
				oos = new ObjectOutputStream(socket.getOutputStream());
				oos.writeObject(msg);
				oos.close();
				socket.close();
			}
			ois.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
