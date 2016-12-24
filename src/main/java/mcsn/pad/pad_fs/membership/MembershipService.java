package mcsn.pad.pad_fs.membership;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LocalGossipMember;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.hpclab.consistent.ConsistentHasherImpl;
import mcsn.pad.pad_fs.common.Configuration;

/**
 * 
 * @author Leonardo Gazzarri
 *
 * This is just a "Proxy" for the GossipService implemented by Edward Capriolo
 */
public class MembershipService implements IMembershipService {
	
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
		if (!isRunning) {
			isRunning = true;
			gossipService.start();
		}
	}
	
	@Override
	public void shutdown() {
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
			members.add(new Member(member.getHost(), member.getPort(), member.getHeartbeat(), member.getHost()));
		}
		return members;
	}
	
	@Override
	public List<Member> getPreferenceList() throws InterruptedException {
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
		for (Map.Entry<Member, List<String>> e : map.entrySet()) {
			if (e.getValue().size() != 0) {
				coordinator = e.getKey();
				break;
			}
		}
		return coordinator;
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
