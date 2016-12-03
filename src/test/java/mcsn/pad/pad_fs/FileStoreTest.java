package mcsn.pad.pad_fs;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.storage.local.FileStore;
import voldemort.versioning.Versioned;

public class FileStoreTest {
	
	private SecureRandom random = new SecureRandom();
	
	private int dim = 10;

	public String nextSessionId() {
	    return new BigInteger(100, random).toString(32);
	}

	@Test
	public void putGetTest() throws IOException {
		Path path = Paths.get("/tmp/processing/");
		if (!Files.isDirectory(path))
			Files.createDirectory(path);
		
		FileStore fs = new FileStore(path);
		Serializable key = nextSessionId();
		Versioned<byte[]> value = new Versioned<>("ciao".getBytes());
		
		fs.put(key, value);
		
		Versioned<byte[]> getValue = fs.get(key);
		String str = new String(getValue.getValue(), StandardCharsets.UTF_8);
		Assert.assertTrue(str, str.equals("ciao"));
	}
	
	@Test
	public void listTest() throws IOException {
		Path path = Paths.get("/tmp/processing/");
		if (!Files.isDirectory(path))
			Files.createDirectory(path);
		
		Map<Serializable, Versioned<byte[]>> objs = new HashMap<>();
		for (int i=0; i<dim; i++) {
			objs.put(nextSessionId(), new Versioned<>(nextSessionId().getBytes()));
		}

		FileStore fs = new FileStore(path);
		for (Serializable key: objs.keySet()) {
			fs.put(key, objs.get(key));
		}
		
		Map<Serializable, Versioned<byte[]>> map = fs.list(objs.keySet());
		for (Serializable key: objs.keySet()) {
			byte[] a = objs.get(key).getValue();
			byte[] b = map.get(key).getValue();
			Assert.assertTrue(Arrays.equals(a, b));
		}
	}
}
