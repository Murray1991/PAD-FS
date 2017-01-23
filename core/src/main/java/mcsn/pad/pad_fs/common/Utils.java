package mcsn.pad.pad_fs.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import it.cnr.isti.hpclab.consistent.ConsistentHasher.BytesConverter;
import junit.framework.Assert;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class Utils {

	public static BytesConverter<Versioned<byte[]>> getVersionedObjToBytesConverter() {
		
		return new BytesConverter<Versioned<byte[]>>() {
			@Override
			public byte[] convert(Versioned<byte[]> data) {
				Assert.assertTrue(data != null);
				byte[] versioned_data = null;
				try {
					versioned_data = convertToBytes(data);
				} catch (IOException e) {
					e.printStackTrace();
				}
				return versioned_data;
			}
		};
		
	}
	
	public static byte[] convertToBytes(Object object) throws IOException {
	    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
	         ObjectOutput out = new ObjectOutputStream(bos)) {
	        out.writeObject(object);
	        return bos.toByteArray();
	    } 
	}
	
	public static Object convertFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
	    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
	         ObjectInput in = new ObjectInputStream(bis)) {
	        return in.readObject();
	    } 
	}
	
	public static <T> int compare(Versioned<T> v1, Versioned<T> v2) {
		if (v2 == null)
			return 1;
		VectorClock vc1 = (VectorClock) v1.getVersion();
		VectorClock vc2 = (VectorClock) v2.getVersion();
		Occurred occurr = vc1.compare(vc2);
		if (occurr == Occurred.BEFORE && !vc1.equals(vc2)) {
			return -1;
		}
		if (occurr == Occurred.AFTER) {
			return 1;
		}
		//if (occurr == Occurred.CONCURRENTLY)
		//	System.out.println("-- Concurrency case...");
		return 0;	//case concurrency or vc1.equals(vc2)
	}
	
	public static <T> int compareTimestamps(Versioned<T> v1, Versioned<T> v2) {
		if (v2 == null)
			return 1;
		long t1 = ((VectorClock) v1.getVersion()).getTimestamp();
		long t2 = ((VectorClock) v2.getVersion()).getTimestamp();
		if (t1 < t2)
			return -1;
		if (t1 > t2)
			return 1;
		return 0;
	}
}
