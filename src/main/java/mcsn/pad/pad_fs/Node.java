package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONException;

import mcsn.pad.pad_fs.common.Configuration;
import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.membership.MembershipService;
import mcsn.pad.pad_fs.storage.StorageService;

/**
 * 
 * @author Leonardo Gazzarri
 * 
 * This class represents a node of a PAD-FS distributed storage system.
 *
 */
public class Node 
{
	public static final Logger LOGGER = Logger.getLogger(Node.class);
	
	private Configuration config;
	
	private List<IService> services;

	private MembershipService membershipService;
    
    public Node(File configFile) throws FileNotFoundException, JSONException, IOException, InterruptedException {
    	config = new Configuration(configFile);
    	services = createServices();
	}
    
	public Node(final int port, String seed) throws UnknownHostException, InterruptedException {
		config = new Configuration(seed, port);
		services = createServices();
	}

	public Node(String ipAddress, File configFile) throws FileNotFoundException, JSONException, IOException, InterruptedException {
		config = new Configuration (ipAddress, configFile);
		services = createServices();
	}
	
	private List<IService> createServices() throws UnknownHostException, InterruptedException {
		List<IService> services = new ArrayList<>();
		membershipService = 
				new MembershipService( config.getHost(),
						config.getPort(),
						config.getLogLevel(),
						config.getGossipMembers(),
						config.getGossipSettings());
		
		services.add(membershipService);
		
		StorageService storageService = 
				new StorageService(membershipService);
		
		services.add(storageService);
		
		return services;
	}
	
	public void start() {
		for (IService service : services )
			service.start();
	}
	
	public void shutdown() {
		for (IService service : services )
			service.shutdown();
	}
	
	public List<Member> getMemberList() {
		return membershipService.getMembers();
	}
	
	public Member getMyself() {
		return membershipService.getMyself();
	}
}
