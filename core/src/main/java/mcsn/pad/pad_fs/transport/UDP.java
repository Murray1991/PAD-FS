package mcsn.pad.pad_fs.transport;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.SourceMessage;

public class UDP {
	public static void send(Message msg, InetSocketAddress addr) throws IOException {
	    
		byte[] data = Utils.convertToBytes(msg);
	    byte[] length = new byte[4];
	    // int -> byte[]
	    for (int i = 0; i < 4; ++i) {
	    	int shift = i << 3; // i * 8
	    	length[3-i] = (byte)((data.length & (0xff << shift)) >>> shift);
	    }
	    
		ByteBuffer byteBuffer = ByteBuffer.allocate(4 + data.length);
		byteBuffer.put(length);
		byteBuffer.put(data);
		byte[] buf = byteBuffer.array();

		//TODO What if data.length is bigger than the maximum pkt size?
		DatagramSocket socket = new DatagramSocket();
		DatagramPacket pkt = new DatagramPacket(buf, buf.length, addr);
		socket.send(pkt);
		socket.close();	
	}
	
	public static void send(Message msg, DatagramSocket socket, InetSocketAddress addr) throws IOException {
		byte[] data = Utils.convertToBytes(msg);
	    byte[] length = new byte[4];
	    // int -> byte[]
	    for (int i = 0; i < 4; ++i) {
	    	int shift = i << 3; // i * 8
	    	length[3-i] = (byte)((data.length & (0xff << shift)) >>> shift);
	    }
	    
		ByteBuffer byteBuffer = ByteBuffer.allocate(4 + data.length);
		byteBuffer.put(length);
		byteBuffer.put(data);
		byte[] buf = byteBuffer.array();

		//TODO what if data.length is bigger than the maximum pkt size?
		DatagramPacket pkt = new DatagramPacket(buf, buf.length, addr);
		socket.send(pkt);
	}
	
	public static Message receive(DatagramSocket socket) throws ClassNotFoundException, IOException {
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
	
	public static SourceMessage srcReceive(DatagramSocket socket) throws ClassNotFoundException, IOException {
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
	    return new SourceMessage(m, p);
	}

	public static SourceMessage srcReceive(DatagramSocket socket, int timeout) throws IOException, SocketTimeoutException, ClassNotFoundException {
		byte[] buf = new byte[socket.getReceiveBufferSize()];
		DatagramPacket p = new DatagramPacket(buf, buf.length);
		socket.setSoTimeout(timeout);
		socket.receive(p);
		
		int packet_length = 0;
		for (int i = 0; i < 4; i++) {
			int shift = (4 - 1 - i) * 8;
			packet_length += (buf[i] & 0x000000FF) << shift;
		}
		
		Assert.assertEquals(true, (packet_length == (p.getLength()-4)));
		byte[] bytes = Arrays.copyOfRange(buf, 4, p.getLength());
	    Message m = (Message) Utils.convertFromBytes(bytes);
	    return new SourceMessage(m, p);
	}
}
