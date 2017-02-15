package mcsn.pad.pad_fs;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mcsn.pad.pad_fs.storage.local.ActiveMapDBStore;
import mcsn.pad.pad_fs.utils.TestUtils;
import voldemort.versioning.Versioned;

public class ActiveMapDBStoreTest {

	final String dbName = "dbTest";
	ActiveMapDBStore db;
	int num = 110;
	
	@Before
	public void setup() {
		db = new ActiveMapDBStore(dbName);
	}
	
	@After
	public void teardown() {
		try {
			File file = new File("dbTest");
			Files.deleteIfExists(file.toPath());
		} catch (IOException e) {
		}
		db.close();
	}
	

	/**
	 * just a test to check the times
	 * @throws InterruptedException 
	 */
	@Test
	public void testPutTimes() throws InterruptedException {
		
		long start, delta;
		Map<Serializable, Versioned<byte[]>> map = new HashMap<>();
		for (int i = 0; i < num; i++) {
			Versioned<byte[]> v = TestUtils.getVersioned(TestUtils.getRandomString().getBytes());
			map.put(Integer.toString(v.hashCode()), v);
		}
		
		System.out.println("times to insert " + num + " items in the db");
		for (Serializable k : map.keySet()) {
			start = System.nanoTime();
			db.put(k, map.get(k));
			delta = System.nanoTime() - start;
			System.out.println("put: " + TimeUnit.NANOSECONDS.toMillis(delta));
		}
		
		System.out.println("wait few seconds");
		Thread.sleep(3000);
		
		System.out.println("check if keys exist");
		for (Serializable k : map.keySet()) {
			Assert.assertTrue( db.get(k) != null );
		}
	}
}
