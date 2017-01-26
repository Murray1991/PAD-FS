package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.junit.Assert;

import mcsn.pad.pad_fs.common.Configuration;
import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.membership.MembershipService;
import mcsn.pad.pad_fs.server.ServerService;
import mcsn.pad.pad_fs.storage.StorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;

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

	private Path dirPath;
	
	private Node(Configuration config, Path dirPath, Class<? extends LocalStore> storeClass) throws UnknownHostException, InterruptedException {
		this.config = config;
		this.dirPath = dirPath;
		this.services = createServices(storeClass);
	}
	
	public Node(File configFile, Class<? extends LocalStore> storeClass) throws FileNotFoundException, JSONException, IOException, InterruptedException {
    	this( new Configuration(configFile) , Paths.get(""), storeClass );
	}
    
	public Node(final int port, String seed, Class<? extends LocalStore> storeClass) throws UnknownHostException, InterruptedException {
		this( new Configuration(seed, port) , Paths.get(""), storeClass  );
	}

	public Node(String ipAddress, File configFile, Class<? extends LocalStore> storeClass) throws FileNotFoundException, JSONException, IOException, InterruptedException {
		this( new Configuration (ipAddress, configFile) , Paths.get("") , storeClass  );
	}
	
	public Node(File configFile, Path dirPath, Class<? extends LocalStore> storeClass) throws FileNotFoundException, JSONException, IOException, InterruptedException {
    	this( new Configuration(configFile) , dirPath, storeClass );
	}
    
	public Node(final int port, String seed, Path dirPath, Class<? extends LocalStore> storeClass) throws UnknownHostException, InterruptedException {
		this( new Configuration(seed, port) , dirPath, storeClass  );
	}

	public Node(String ipAddress, File configFile, Path dirPath, Class<? extends LocalStore> storeClass) throws FileNotFoundException, JSONException, IOException, InterruptedException {
		this( new Configuration (ipAddress, configFile) , dirPath , storeClass  );
	}
	
	private List<IService> createServices(Class<? extends LocalStore> storeClass) throws UnknownHostException, InterruptedException {
		List<IService> services = new ArrayList<>();
		membershipService = 
				new MembershipService( config.getHost(),
						config.getPort(),
						config.getLogLevel(),
						config.getGossipMembers(),
						config.getGossipSettings());
		
		services.add(membershipService);
		
		LocalStore dbStore = null;
		try {
			
			Constructor<?> constructor = storeClass.getConstructor(String.class);
			dbStore = (LocalStore) constructor.newInstance( 
							new Object[] { Paths.get( dirPath.toString(), config.getHost() + ".db" ).toString() });
			
		} catch (InstantiationException 
				| IllegalAccessException 
				| IllegalArgumentException 
				| InvocationTargetException
				| NoSuchMethodException 
				| SecurityException e) {
			e.printStackTrace();
		}
		
		Assert.assertNotNull("local store is null", dbStore);
		Assert.assertNotNull("next local store is null", dbStore.getNext());
		
		StorageService storageService = 
				new StorageService(membershipService, dbStore);
		
		services.add(storageService);
		
		InetAddress bindAddr = InetAddress.getByName(config.getHost());
		ServerService serverService =
				new ServerService(storageService, 8080, 0, bindAddr);
		
		services.add(serverService);
		
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
