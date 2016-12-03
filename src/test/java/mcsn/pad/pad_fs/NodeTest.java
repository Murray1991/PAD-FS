package mcsn.pad.pad_fs;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.junit.Test;


public class NodeTest {
	

	public int dim = 4;

	@Test
	public void test() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		
			String filename = this.getClass().getResource("/gossip.conf").getFile();
			File configFile = new File(filename);
			List<Node> members = new ArrayList<>();
			
			for (int i = 1; i <= dim; i++) {
				members.add(i-1,  new Node("127.0.0."+i, configFile));
				
			}
			
			members.add(dim++, new Node("127.0.0.10", configFile));
			members.add(dim++, new Node("127.0.0.11", configFile));
			
			System.out.println("starting nodes...");
			for (Node node : members)
				node.start();
			
			Thread.sleep(10000);
			
			for (int i = 0; i < dim; i++) {
				int x = members.get(i).getMemberList().size();
				
				System.out.println("#members = " + x);
				assertTrue("gossip failed: " + x + " members instead of " + (dim-1) + " expected", x == dim-1);
			}
			
			
			assertTrue(true);
			
			for (Node n : members)
				n.shutdown();
		
	}

}
