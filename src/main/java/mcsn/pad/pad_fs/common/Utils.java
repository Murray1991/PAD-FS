package mcsn.pad.pad_fs.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import com.google.common.base.Preconditions;

import it.cnr.isti.hpclab.consistent.ConsistentHasher.BytesConverter;
import voldemort.versioning.Versioned;

public class Utils {

	public static BytesConverter<Versioned<byte[]>> getVersionedObjToBytesConverter() {
		
		return new BytesConverter<Versioned<byte[]>>() {
			@Override
			public byte[] convert(Versioned<byte[]> data) {
				Preconditions.checkNotNull(data);
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
	
}
