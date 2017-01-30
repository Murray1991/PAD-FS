package mcsn.pad.pad_fs.membership;

import java.lang.Thread.State;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LocalGossipMember;

import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Configuration;

/**
 * 
 * @author Leonardo Gazzarri
 *
 * This is just a "Proxy" for the GossipService implemented by Edward Capriolo
 */
public class MembershipService implements IMembershipService {
	
	public static final Logger LOGGER = Logger.getLogger("MembershipService");
	
	/* utilizes a gossip protocol for membership information and failure detection */
	private GossipService gossipService;
	
	private String host;
	private int port;
	private List<GossipMember> gossipMembers;
	private GossipSettings gossipSettings;
	
	private boolean isRunning = false;
	
	public MembershipService(String host, int port, int logLevel, List<GossipMember> gossipMembers, GossipSettings gossipSettings) throws UnknownHostException, InterruptedException {	
		this.host = host;
		this.port = port;
		this.gossipMembers = gossipMembers;
		this.gossipSettings = gossipSettings;
		
		gossipService = new GossipService(host, port, host, gossipMembers, gossipSettings, null);
	}
	
	public MembershipService(Configuration config) throws UnknownHostException, InterruptedException {
		this( config.getHost(), config.getPort(), config.getLogLevel(), config.getGossipMembers(), config.getGossipSettings());
	}

	@Override
	public void start() {
		State state = gossipService.get_gossipManager().getState();
		if (! state.equals(State.NEW) ) {
			try {
				gossipService = new GossipService(host, port, host, gossipMembers, gossipSettings, null);
			} catch (UnknownHostException | InterruptedException e) {
				System.err.println("ERRORISSIMO IN START");
			}
		}
		if (!isRunning) {
			LOGGER.info(this + " -- start");
			isRunning = true;
			gossipService.start();
		}
	}
	
	@Override
	public void shutdown() {
		if (isRunning) {
			LOGGER.info(this + " -- shutdown");
			isRunning = false;
			gossipService.shutdown();
		}
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}
	
	@Override
	public Member getMyself() {
		LocalGossipMember member = gossipService
				.get_gossipManager()
				.getMyself();
		
		Assert.assertTrue(member.getHost().equals(host));
		return new Member(member.getHost(), member.getPort(), member.getHeartbeat(), member.getHost());
	}
	
	@Override
	public List<Member> getMembers() {
		
		List<Member> members = new ArrayList<>();
		List<LocalGossipMember> localMembers = gossipService
				.get_gossipManager()
				.getMemberList();
		
		Member myself = getMyself();
		for (LocalGossipMember member : localMembers) {
			Member m = new Member(member.getHost(), 
					member.getPort(), 
					member.getHeartbeat(), 
					member.getHost());
			
			if (! m.equals(myself)) {
				members.add(m);
			}
			
		}
		return members;
	}
	
	@Override
	public List<Member> getDeadMembers() {
		List<Member> deadMembers = new ArrayList<>();
		List<LocalGossipMember> localMembers = gossipService
				.get_gossipManager()
				.getDeadList();
		
		for (LocalGossipMember member : localMembers) {
			Member m = new Member(member.getHost(), member.getPort(), member.getHeartbeat(), member.getHost());
			deadMembers.add(m);
		}
		
		return deadMembers;
	}
	
	@Override
	public List<Member> getPreferenceList(String key, int N) throws InterruptedException {
		List<Member> members = getMembers();
		members.add(getMyself());
		return PartitionUtils.getPreferenceList(members, key, N);
	}
	
	@Override
	public Member getCoordinator(String key) {
		List<Member> members = getMembers();
		members.add(getMyself());
		return PartitionUtils.getCoordinator(members, key);
	}
	
	@Override
	public String toString() {
		return "MembershipService@"+getMyself();
	}
}
