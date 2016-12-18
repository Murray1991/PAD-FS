package mcsn.pad.pad_fs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.ArrayList;

import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.server.ServerService;
import mcsn.pad.pad_fs.utils.DummyService;
import mcsn.pad.pad_fs.utils.TestUtils;
import voldemort.versioning.Versioned;

public class ServerServiceTest {

	@Test
	public void test() throws IOException, ClassNotFoundException, InterruptedException {
		
		InetAddress addr = InetAddress.getByName("127.0.0.1");
		
		// start the server
		ServerService server = new ServerService(new DummyService(), 8080, 0, addr);
		server.start();
		
		// build test elements
		ArrayList<Versioned<byte[]>> list = (ArrayList<Versioned<byte[]>>) TestUtils.getElements(10);
		
		// start sequentially tests
		for ( Versioned<byte[]> e : list ) {
			Socket sck = new Socket(addr, 8080);
			Message sendMsg = new Message(1, TestUtils.nextSessionId(new SecureRandom()), e);
			Message rcvMsg = request(sendMsg, sck);
			boolean b = 
					sendMsg.key.equals(rcvMsg.key) &&
					sendMsg.type == rcvMsg.type && 
					! sendMsg.value.equals(rcvMsg.value);
			Assert.assertTrue(b);
			sck.close();
		}
		
		server.shutdown();

	}
	
	private Message request(Message sendMsg, Socket sck) throws IOException, ClassNotFoundException {
		ObjectOutputStream oos = new ObjectOutputStream(sck.getOutputStream());
		oos.writeObject(sendMsg);
		
		ObjectInputStream ois = new ObjectInputStream(sck.getInputStream());
		Message msg = (Message) ois.readObject();
		
		ois.close();
		oos.close();
		return msg;
	}
}
