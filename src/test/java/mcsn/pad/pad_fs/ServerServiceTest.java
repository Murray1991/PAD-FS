package mcsn.pad.pad_fs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.server.ServerService;
import mcsn.pad.pad_fs.utils.DummyService;
import mcsn.pad.pad_fs.utils.TestUtils;
import voldemort.versioning.Versioned;

public class ServerServiceTest {
	
	private ServerService server;
	private InetAddress addr;
	private int dim = 100;
	
	@Before 
	public void setup() throws UnknownHostException {
		System.out.println("-- setup ServerServiceTest: DummyService used introduces a delay of 500ms for request");
		this.addr = InetAddress.getByName("127.0.0.1");
		this.server = new ServerService(new DummyService(), 8080, 0, addr);
		server.start();
	}
	
	@After
	public void teardown() {
		System.out.println("-- teardown ServerServiceTest");
		server.shutdown();
	}

	@Test
	public void sequentialTest() throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("-- starting sequentialTest: completion time expected is ~50s");
		
		// build test elements
		List<Versioned<byte[]>> list = 
				(ArrayList<Versioned<byte[]>>) TestUtils.getElements(dim);
		
		for ( Versioned<byte[]> e : list ) {
			Socket sck = new Socket(addr, 8080);
			ClientMessage sendMsg = new ClientMessage(1, TestUtils.nextSessionId(new SecureRandom()), e);
			ClientMessage rcvMsg = request(sendMsg, sck);
			boolean b = 
					sendMsg.key.equals(rcvMsg.key) &&
					sendMsg.type == rcvMsg.type && 
					! sendMsg.value.equals(rcvMsg.value);
			//The server returns a properly formatted message according to the DummyService
			Assert.assertTrue(b);
			sck.close();
		}
		
	}
	
	@Test
	public void threadTest() throws IOException, ClassNotFoundException, InterruptedException  {
		System.out.println("-- starting threadTest: completion time expected is ~1s");
		
		// build test elements
		List<Versioned<byte[]>> list = 
				(ArrayList<Versioned<byte[]>>) TestUtils.getElements(dim);
		
		Executor executor = Executors.newFixedThreadPool(dim/2);
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
		// All the transactions should be OK
		Assert.assertTrue(errors == 0);
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
