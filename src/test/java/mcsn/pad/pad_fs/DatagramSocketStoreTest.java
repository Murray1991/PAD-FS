package mcsn.pad.pad_fs;

import java.io.IOException;
import java.math.BigInteger;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.storage.remote.DatagramSocketStore;
import mcsn.pad.pad_fs.storage.remote.Message;
import voldemort.versioning.Versioned;

public class DatagramSocketStoreTest {
	
	private int dim = 10;
	
	private SecureRandom random = new SecureRandom();
	
	public String nextSessionId() {
	    return new BigInteger(128, random).toString(32);
	}
	
	private class SRunnable extends Thread {
		
		private ArrayList<Versioned<byte[]>> list;
		private InetSocketAddress addr;

		public SRunnable(InetSocketAddress addr, ArrayList<Versioned<byte[]>> list) {
			this.addr = addr;
			this.list = list;
		}
		
		private Message receive(DatagramSocket socket) throws ClassNotFoundException, IOException {
			
			byte[] buf = new byte[socket.getReceiveBufferSize()];
			DatagramPacket p = new DatagramPacket(buf, buf.length);
			socket.receive(p);
			
			int packet_length = 0;
			for (int i = 0; i < 4; i++) {
				int shift = (4 - 1 - i) * 8;
				packet_length += (buf[i] & 0x000000FF) << shift;
			}
			
			Assert.assertEquals(true, (packet_length == (p.getLength()-4)));
			byte[] bytes = Arrays.copyOfRange(buf, 4, p.getLength());
		    Message m = (Message) Utils.convertFromBytes(bytes);
		    return m;
		}

		@Override
		public void run() {
			
			System.out.println("Thread code");
			System.out.println("list size = " + list.size());
			
			DatagramSocket s = null;
			try {
				
				s = new DatagramSocket(addr);
				for (int i=0; i < list.size(); i++ ) {
					Message m = receive(s);
					System.out.println("at " + i + ", received: " + m.value + " " + list.contains(m.value));
					Assert.assertEquals("error", true, list.contains(m.value));
				}
			} catch (SocketException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			if (s != null)
				s.close();
		}
	}
	
	@Test
	public void store_put() throws ClassNotFoundException, IOException {
		
		InetSocketAddress addr = new InetSocketAddress("127.0.0.5", 2002);
		ArrayList<Versioned<byte[]>> list = new ArrayList<>();
		
		for (int i=0; i<dim; i++) {
			list.add( new Versioned<byte[]>( nextSessionId().getBytes()) );
		}
		
		SRunnable t = new SRunnable(addr, list);
		t.start();
		
		DatagramSocketStore s = new DatagramSocketStore(addr);
		for (Versioned<byte[]> e : list) {
			s.put(nextSessionId(), e);
		}
		
		s.close();
	}
}
