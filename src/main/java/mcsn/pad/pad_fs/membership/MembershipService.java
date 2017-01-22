package mcsn.pad.pad_fs.membership;

import java.lang.Thread.State;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.util.logging.Logger;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LocalGossipMember;
import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.hpclab.consistent.ConsistentHasherImpl;
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
		LOGGER.info(this + " -- start");
		State state = gossipService.get_gossipManager().getState();
		if (! state.equals(State.NEW) ) {
			try {
				gossipService = new GossipService(host, port, host, gossipMembers, gossipSettings, null);
			} catch (UnknownHostException | InterruptedException e) {
				System.err.println("ERRORISSIMO IN START");
			}
		}
		
		if (!isRunning) {
			isRunning = true;
			gossipService.start();
		}
	}
	
	@Override
	public void shutdown() {
		LOGGER.info(this + " -- shutdown");
		if (isRunning) {
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
		LocalGossipMember member = gossipService.get_gossipManager().getMyself();
		return new Member(member.getHost(), member.getPort(), member.getHeartbeat(), member.getHost());
	}
	
	@Override
	public List<Member> getMembers() {
		List<Member> members = new ArrayList<>();
		List<LocalGossipMember> localMembers = gossipService.get_gossipManager().getMemberList();
		for (LocalGossipMember member : localMembers) {
			//TODO controllare se faccio bene...
			Member m = new Member(member.getHost(), member.getPort(), member.getHeartbeat(), member.getHost());
			if (!getMyself().equals(m)) {
				members.add(m);
			}
		}
		return members;
	}
	
	@Override
	public List<Member> getPreferenceList() throws InterruptedException {
		//TODO do this with virtual nodes and a degree factor
		ConsistentHasher<Member, String> cHasher = getConsistentHasher("a");
		List<Member> sortedBuckets = new ArrayList<Member>();
		Map<Member, List<String>> map;
		while (cHasher.getAllBuckets().size() != 0) {
			map = cHasher.getAllBucketsToMembersMapping();
			for (Map.Entry<Member, List<String>> e : map.entrySet()) {
				if (e.getValue().size() != 0 || cHasher.getAllBuckets().size() == 1) {
					sortedBuckets.add(e.getKey());
					cHasher.removeBucket(e.getKey());
					break;
				}
			}
		}
		return sortedBuckets;
	}
	
	@Override
	public Member getCoordinator(String key) {
		Member coordinator = null;
		ConsistentHasher<Member, String> cHasher = getConsistentHasher(key);
		Map<Member, List<String>> map = cHasher.getAllBucketsToMembersMapping();
		Assert.assertTrue(map.size() > 0);
		for (Map.Entry<Member, List<String>> e : map.entrySet()) {
			if (e.getValue().size() != 0) {
				coordinator = e.getKey();
				break;
			}
		}
		//TODO investigare... la ragione per cui coordinator puo' essere null
		return coordinator == null ? getMyself() : coordinator;
	}
	
	@Override
	public String toString() {
		return "MembershipService@"+getMyself();
	}
	
	private ConsistentHasher<Member, String> getConsistentHasher(String key) {
		ConsistentHasher<Member, String> cHasher = new ConsistentHasherImpl<>(
				1, 
				new MemberByteConverter(), 
				ConsistentHasher.getStringToBytesConverter(),
				ConsistentHasher.SHA1);
		List<Member> list = getMembers();
		list.add(getMyself());
		for (Member m : list)
			cHasher.addBucket(m);
		cHasher.addMember(key);
		return cHasher;
	}
}
