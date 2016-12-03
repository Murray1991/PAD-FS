package mcsn.pad.pad_fs;

import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LogLevel;
import com.google.code.gossip.RemoteGossipMember;
import com.google.code.gossip.event.GossipListener;
import com.google.code.gossip.event.GossipState;

public class GossipTest {

	@Test
	public void test() throws UnknownHostException, InterruptedException {
		
		GossipSettings settings = new GossipSettings();
		
		// startupMembers is just a list of places to initially connect to
		int seedNodes = 3;
		List<GossipMember> startupMembers = new ArrayList<GossipMember>();
		for (int i = 1; i <= seedNodes; i++) {
		    startupMembers.add(
		    		new RemoteGossipMember("127.0.0." + i, 2000, i + ""));
		}
		
		// clients is the list of gossip processes
		int clusterMembers = 5;
		List<GossipService> clients = new ArrayList<GossipService>();
		for (int i = 1; i <= clusterMembers; i++) {
			GossipService gossipService = 
					new GossipService(
							"127.0.0." + i, 2000, i + "", 
							LogLevel.DEBUG, startupMembers, settings,
							new GossipListener(){
								public void gossipEvent(GossipMember member, GossipState state) {
									System.out.println(member.getId()+" "+ member + " " + state);
									}
								});
			
		    clients.add(gossipService);
		    gossipService.start();
		}
		
		Thread.sleep(10000);
		
		for (int i = 0; i < clusterMembers; ++i) {
			int x = clients.get(i).get_gossipManager().getMemberList().size();
			assertTrue("gossip failed", x == clusterMembers-1);
		}
		
		assertTrue(true);
	}

}
