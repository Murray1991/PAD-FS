package mcsn.pad.pad_fs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.server.ServerService;
import mcsn.pad.pad_fs.utils.DummyService;
import mcsn.pad.pad_fs.utils.TestUtils;
import voldemort.versioning.Versioned;

public class ServerServiceThreadTest {

	@Test
	public void test() throws IOException, ClassNotFoundException, InterruptedException  {

		InetAddress addr = InetAddress.getByName("127.0.0.1");
		// start the server
		ServerService server = new ServerService(new DummyService(), 8080, 0, addr);
		server.start();
		
		// build test elements
		ArrayList<Versioned<byte[]>> list = (ArrayList<Versioned<byte[]>>) TestUtils.getElements(100);
		
		Executor executor = Executors.newFixedThreadPool(100);
		CompletionService<Boolean> completionService = 
		       new ExecutorCompletionService<Boolean>(executor);
		
		for ( Versioned<byte[]> e : list) {
			ClientMessage sendMsg = new ClientMessage(1, TestUtils.nextSessionId(new SecureRandom()), e);
			completionService.submit(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					Socket sck = new Socket(addr, 8080);
					ClientMessage rcvMsg = request(sendMsg, sck);
					sck.close();
					boolean b = 
							sendMsg.key.equals(rcvMsg.key) &&
							sendMsg.type == rcvMsg.type && 
							! sendMsg.value.equals(rcvMsg.value);
					return new Boolean(b);
				}
			});
		}
		
		int errors = 0;
		for(int received = 0; received < list.size() && errors == 0; received++) {
	      Future<Boolean> resultFuture = completionService.take();
	      Boolean result;
	      try {
			result = resultFuture.get();
			Assert.assertTrue(result.booleanValue());
	      } catch (ExecutionException e1) {
			errors++;
	      }
		}
		Assert.assertTrue(errors == 0);
		server.shutdown();
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

}
