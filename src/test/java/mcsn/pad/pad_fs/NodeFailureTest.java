package mcsn.pad.pad_fs;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.json.JSONException;
import org.junit.Test;

public class NodeFailureTest {

	@Test
	public void test() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		
		try {
			
			String filename = this.getClass().getResource("/gossip.conf").getFile();
			
			File configFile = new File(filename);
			ArrayList<Node> members = new ArrayList<>();
			
			/* start the startup members present in the gossip.conf file*/
			int dim = 4;
			for (int i = 1; i <= dim; i++) {
				members.add(i-1,  new Node("127.0.0."+i, configFile));
			}
			
			/* start other two members */
			members.add(dim++, new Node("127.0.0.10", configFile));
			members.add(dim++, new Node("127.0.0.11", configFile));
			
			Thread.sleep(10000);
			
			/* delete two members at random */
			Random rand = new Random();
			int index = rand.nextInt(dim--);
			members.get(index).shutdown();
			members.remove(index);
			
			index = rand.nextInt(dim--);
			members.get(index).shutdown();
			members.remove(index);
			
			Thread.sleep(15000);
			
			for (int i = 0; i < dim; i++) {
				int x = members.get(i).getMemberList().size();
				System.out.println("#members = " + x);
				assertTrue("#members = " + x, x == dim-1);
			}
		
		} catch (NullPointerException e) {
			assertTrue("gossip.conf not found", false);
		}
	}

}
