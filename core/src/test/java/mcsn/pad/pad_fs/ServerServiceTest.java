package mcsn.pad.pad_fs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
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
import mcsn.pad.pad_fs.message.client.PutMessage;
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
		this.addr = InetAddress.getByName("127.0.0.1");
		this.server = new ServerService(new DummyService(), 8080, 0, addr);
		server.start();
	}
	
	@After
	public void teardown() {
		server.shutdown();
	}

	@Test
	public void sequentialTest() throws IOException, ClassNotFoundException, InterruptedException {
		
		System.out.println("-- starting sequentialTest: completion time expected is ~50s");
		for ( Versioned<byte[]> value : TestUtils.getElements(dim) ) {
			Socket sck = new Socket(addr, 8080);
			String key = TestUtils.getRandomString();
			PutMessage sendMsg = new PutMessage(key, value);
			PutMessage rcvMsg = (PutMessage) request(sendMsg, sck);
			boolean b = sendMsg.key.equals(rcvMsg.key) &&
					sendMsg.type == rcvMsg.type && 
					! sendMsg.value.equals(rcvMsg.value);
			/* the server returns a properly formatted message according to the DummyService */
			Assert.assertTrue(b);
			sck.close();
		}
		
	}
	
	@Test
	public void threadTest() throws IOException, ClassNotFoundException, InterruptedException  {
		
		System.out.println("-- starting threadTest: completion time expected is ~1s");
		
		Executor executor = Executors.newFixedThreadPool(dim/2);
		CompletionService<Boolean> completionService = 
		       new ExecutorCompletionService<Boolean>(executor);
		
		for ( Versioned<byte[]> value : TestUtils.getElements(dim)) {
			String key = TestUtils.getRandomString();
			PutMessage msg = new PutMessage(key, value);
			
			completionService.submit( () -> {
				Socket sck = new Socket(addr, 8080);
				PutMessage res = (PutMessage) request(msg, sck);
				sck.close();
				boolean b = msg.key.equals(res.key) &&
						msg.type == res.type && 
						! msg.value.equals(res.value);
				return new Boolean(b);
			});
		}
		
		/* check the results */
		int errors = 0;
		for(int received = 0; received < dim && errors == 0; received++) {
	      Future<Boolean> resultFuture = completionService.take();
	      try {
	    	  Boolean result = resultFuture.get();
	    	  Assert.assertTrue(result.booleanValue());
	      } catch (ExecutionException e1) {
	    	  errors++;
	      }
		}
		
		/* all the transactions should be OK */
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
