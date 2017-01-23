package mcsn.pad.pad_fs;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import mcsn.pad.pad_fs.storage.local.HashMapStore;


public class NodeTest {
	
	private final int size = 4;
	private List<Node> nodes = new ArrayList<>();
	private int dim = size;
	
	@Before
	public void setup() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		System.out.println("-- setup NodeTest");
		String filename = this.getClass().getResource("/gossip.conf").getFile();
		File configFile = new File(filename);
		this.nodes = new ArrayList<>();
		
		for (int i = 1; i <= dim; i++) {
			nodes.add(i-1,  new Node("127.0.0."+i, configFile, HashMapStore.class));
		}
		
		nodes.add(dim++, new Node("127.0.0.10", configFile, HashMapStore.class));
		nodes.add(dim++, new Node("127.0.0.11", configFile, HashMapStore.class));
		
		System.out.println("starting nodes...");
		for (Node node : nodes)
			node.start();
		
		Thread.sleep(10000);
	}
	
	@After
	public void teardown() {
		System.out.println("-- teardown NodeTest");
		dim = size;
		for (Node n : nodes)
			n.shutdown();
	}

	@Test
	public void nodeGossipTest() throws InterruptedException, FileNotFoundException, JSONException, IOException {
			for (int i = 0; i < dim; i++) {
				int x = nodes.get(i).getMemberList().size();
				assertTrue("#nodes: " + x, x == dim-1);
			}
	}
	
	@Test
	public void nodeFailureGossipTest() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		
		/* delete two nodes at random */
		Random rand = new Random();
		int index = rand.nextInt(dim--);
		nodes.get(index).shutdown();
		nodes.remove(index);
		
		index = rand.nextInt(dim--);
		nodes.get(index).shutdown();
		nodes.remove(index);
		
		Thread.sleep(20000);
		
		/* we should have 3 nodes for node */
		for (int i = 0; i < dim; i++) {
			int x = nodes.get(i).getMemberList().size();
			assertTrue("#nodes = " + x, x == dim-1);
		}
	}
}
