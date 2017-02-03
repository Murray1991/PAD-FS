package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.storage.local.HashMapStore;


public class NodeTest {
	
	private final int size = 4;
	private List<Node> nodes;
	private int dim = size;
	
	@Before
	public void setup() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		String filename = this.getClass().getResource("/gossip.conf").getFile();
		File configFile = new File(filename);
		
		this.nodes = new ArrayList<>();
		for (int i = 1; i <= dim; i++) {
			nodes.add(i-1,  new Node("127.0.0."+i, configFile, HashMapStore.class));
		}
		
		nodes.add(dim++, new Node("127.0.0.10", configFile, HashMapStore.class));
		nodes.add(dim++, new Node("127.0.0.11", configFile, HashMapStore.class));
		
		for (Node node : nodes)
			node.start();
		
		Thread.sleep(10000);
	}
	
	@After
	public void teardown() {
		dim = size;
		for (Node n : nodes)
			n.shutdown();
	}

	@Test
	public void nodeGossipTest() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		for (Node node : nodes) {
			Assert.assertTrue(node.getMemberList().size() == dim-1);
		}
	}
	
	@Test
	public void nodeFailureGossipTest() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		
		/* shutdown and remove the first two nodes*/
		nodes.get(0).shutdown();
		nodes.get(1).shutdown();
		nodes.remove(1);
		nodes.remove(0);
		
		System.out.println("-- wait");
		Thread.sleep(10000);
		
		/* we should have 3 nodes for node */
		for (Node node : nodes) {
			Assert.assertTrue(node.getMemberList().size() == dim-3);
		}
	}
}
