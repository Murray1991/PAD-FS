package mcsn.pad.pad_fs;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import mcsn.pad.pad_fs.storage.local.MapDBStore;
import mcsn.pad.pad_fs.utils.TestUtils;
import voldemort.versioning.Versioned;

public class MapDBStoreTest {
	
	MapDBStore db;
	int num = 100;
	
	@Before
	public void setup() {
		db = new MapDBStore("dbTest");
	}
	
	@After
	public void teardown() {
		try {
			File file = new File("dbTest");
			Files.deleteIfExists(file.toPath());
		} catch (IOException e) {
		}
	}
	

	/**
	 * just a test to check the times
	 */
	@Test
	public void testPutTimes() {
		
		long start, delta;
		List<Versioned<byte[]>> l1 = new ArrayList<>();
		for (int i = 0; i < num; i++) {
			l1.add(TestUtils.getVersioned(TestUtils.getRandomString().getBytes()));
		}
		
		System.out.println("-- times for insert " + num + " items one by one (10 commits)");
		for (Versioned<byte[]> v : l1) {
			start = System.nanoTime();
			db.put(Integer.toString(v.hashCode()), v);
			delta = System.nanoTime() - start;
			System.out.println("put: " + TimeUnit.NANOSECONDS.toMillis(delta));
		}
		
		List<Serializable> keys = new ArrayList<>();
		List<Versioned<byte[]>> values = new ArrayList<>();
		for (int i = 0; i < num; i++) {
			Versioned<byte[]> v = TestUtils.getVersioned(TestUtils.getRandomString().getBytes());
			keys.add(Integer.toString(v.hashCode()));
			values.add(v);
		}
		
		System.out.println("-- times for insert " + num + " items in one shot (1 commit at the end)");
		start = System.nanoTime();
		db.put(keys, values);
		delta = System.nanoTime() - start;
		System.out.println("put: " + TimeUnit.NANOSECONDS.toMillis(delta));
	}

}
