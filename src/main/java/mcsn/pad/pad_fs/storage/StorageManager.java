package mcsn.pad.pad_fs.storage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.hpclab.consistent.ConsistentHasherImpl;
import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.membership.MembershipService;
import mcsn.pad.pad_fs.partitioning.PartitionUtils;
import mcsn.pad.pad_fs.storage.remote.Message;
import voldemort.versioning.Versioned;

public class StorageManager extends Thread {

	/*
	 * Asynctask has to perform filestore or remotestore, according to his
	 * knowledge of the network and partitioning of data...
	 */
	private class AsyncTask implements Runnable {

		private Message msg;

		public AsyncTask(Message msg) {
			this.msg = msg;
		}

		@Override
		public void run() {
			System.out.println("received type: " + msg.type);
		}

	}

	private DatagramSocket udpServer;
	private InetAddress laddr;
	private List<String> members;
	
	private final int port = 3000; //default port
	private final AtomicBoolean isRunning;
	private final ConsistentHasher<String, String> cHasher;
	private final MembershipService membershipService;
	private final ExecutorService taskPool;
	
	public StorageManager(MembershipService membershipService) {
		
		this.membershipService = membershipService;
		taskPool = Executors.newCachedThreadPool(); 	//TODO maybe better choices?
		isRunning = new AtomicBoolean(true);
		cHasher = PartitionUtils.getConsistentHasher();
		
		try {
			laddr = InetAddress.getByName(membershipService.getMyself().host);
			udpServer = new DatagramSocket(port, laddr);
		} catch (SocketException | UnknownHostException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void run() {
		
		while (isRunning.get()) {
			
			try {
				
				Message msg = receive(udpServer);
				//members = PartitionUtils.getPreferenceList(membershipService.getMembers());
				taskPool.execute(new AsyncTask(msg));
				
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	@Override
	public void interrupt() {
		isRunning.set(false);
		System.out.println("shutting down...");
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

}
