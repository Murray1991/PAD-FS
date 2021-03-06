package mcsn.pad.pad_fs.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.storage.IStorageService;

public class ServerThread extends Thread {

    static final Logger logger = Logger.getLogger(ServerThread.class);
    
	private Socket socket;
	private IStorageService storageService;
	
	public ServerThread(Socket socket, IStorageService storageService) {
		this.socket = socket;
		this.storageService = storageService;
	}

	@Override
	public void run() {
		long start = 0, delta = 0;
		ObjectInputStream ois = null;
		ObjectOutputStream oos = null;
		ClientMessage msg = null;
		try {
			ois = new ObjectInputStream(socket.getInputStream()); //TODO use Future and timeout
			msg = (ClientMessage) ois.readObject();
			if (msg != null) {
				logger.debug(this.getId() + ": start serverThread");
				start = System.nanoTime();    
				msg = storageService.deliverMessage(msg);
				delta = System.nanoTime() - start;
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
		
		if (logger.isDebugEnabled()) {
			logger.debug(this.getId() + "time elapsed to process message: " + TimeUnit.NANOSECONDS.toMillis(delta));
			//logger.debug(this.getId() + "client response's size: " + Utils.sizeof(msg));
		}
	}
}
