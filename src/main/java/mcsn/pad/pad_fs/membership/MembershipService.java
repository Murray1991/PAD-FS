package mcsn.pad.pad_fs.membership;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LocalGossipMember;

import mcsn.pad.pad_fs.common.Configuration;
import mcsn.pad.pad_fs.common.IService;

/**
 * 
 * @author Leonardo Gazzarri
 *
 */
public class MembershipService implements IService{
	
	public static final Logger LOGGER = Logger.getLogger(MembershipService.class);
	
	/* utilizes a gossip protocol for membership information and failure detection */
	private GossipService gossipService;
	
	private boolean isRunning = false;
	
	public MembershipService(String host, int port, int logLevel, List<GossipMember> gossipMembers, GossipSettings gossipSettings) throws UnknownHostException, InterruptedException {	
		gossipService = new GossipService(host, port, "", logLevel, gossipMembers, gossipSettings, null);
	}
	
	public MembershipService(Configuration config) throws UnknownHostException, InterruptedException {
		this( config.getHost(), config.getPort(), config.getLogLevel(), config.getGossipMembers(), config.getGossipSettings());
	}

	@Override
	public void start() {
		isRunning = true;
		gossipService.start();
	}
	
	@Override
	public void shutdown() {
		isRunning = false;
		gossipService.shutdown();
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}
	
	public List<Member> getMembers() {
		List<Member> members = new ArrayList<>();
		List<LocalGossipMember> localMembers = gossipService.get_gossipManager().getMemberList();
		for (LocalGossipMember member : localMembers) {
			members.add(new Member(member.getHost(), member.getPort(), member.getHeartbeat(), member.getId()));
		}
		return members;
	}
	
	public Member getMyself() {
		LocalGossipMember member = gossipService.get_gossipManager().getMyself();
		return new Member(member.getHost(), member.getPort(), member.getHeartbeat(), member.getId());
	}
}
