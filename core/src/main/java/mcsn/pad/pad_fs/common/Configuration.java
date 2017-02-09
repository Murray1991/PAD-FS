package mcsn.pad.pad_fs.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.RemoteGossipMember;
import com.google.code.gossip.StartupSettings;

public class Configuration {
	
	private String ipAddress;

	private StartupSettings settings;

	private int port;

	private int storagePort;

	private int serverPort;

	private String id;

	private int gossipInterval;

	private int cleanupInterval;

	private int updateInterval;
	
	public Configuration(String ipAddress, File configFile) throws FileNotFoundException, JSONException, IOException {
		this.ipAddress = ipAddress;
		processJSONFile(configFile, ipAddress);
	}
	
	public Configuration(File configFile) throws UnknownHostException, FileNotFoundException, JSONException, IOException {
		this (InetAddress.getLocalHost().getHostAddress(), configFile);
	}
	
	public String getHost() {
		return ipAddress;
	}
	
	public int getPort() {
		return settings.getPort();
	}
	
	public int getStoragePort() {
		return storagePort;
	}
	
	public int getServerPort() {
		return serverPort;
	}
	
	public int getUpdateInterval() {
		return updateInterval;
	}
	
	public int getLogLevel() {
		return 0;
	}
	
	public List<GossipMember> getGossipMembers() {
		return settings.getGossipMembers();
	}
	
	public GossipSettings getGossipSettings() {
		return settings.getGossipSettings();
	}
	
	private void processJSONFile(File jsonFile, String ipAddress) throws JSONException,
	  FileNotFoundException, IOException {
		
		BufferedReader br = new BufferedReader(new FileReader(jsonFile));
		StringBuffer buffer = new StringBuffer();
		String line;
		while ((line = br.readLine()) != null) {
		  buffer.append(line.trim());
		}
		br.close();
		
		// Lets parse the String as JSON.
		JSONObject jsonObject = new JSONArray(buffer.toString()).getJSONObject(0);
		
		/* port number for the gossip protocol */
		this.port = jsonObject.getInt("port");
		
		/* port number for the storage protocol */
		this.storagePort = jsonObject.getInt("storage_port");
		
		/* port number for the server */
		this.serverPort = jsonObject.getInt("server_port");
		
		/* get the id to be used */
		this.id = ipAddress;
		
		/* get the gossip_interval from the config file */
		this.gossipInterval = jsonObject.getInt("gossip_interval");
		
		/* get the cleanup_interval from the config file. */
		this.cleanupInterval = jsonObject.getInt("cleanup_interval");
		
		/* update interval for the replicaManager*/
		this.updateInterval = jsonObject.getInt("update_interval");
		
		/* get the hostname of the machine which run the gossip service */
		String hostname = ipAddress;
		
		// Initiate the settings with the port number.
		this.settings = new StartupSettings(id, hostname, port, new GossipSettings(
		        gossipInterval, cleanupInterval));
		
		// Now iterate over the members from the config file and add them to the settings.
		// String configMembersDetails = "Config-members [";
		JSONArray membersJSON = jsonObject.getJSONArray("members");
		for (int i = 0; i < membersJSON.length(); i++) {
		  JSONObject memberJSON = membersJSON.getJSONObject(i);
		  RemoteGossipMember member = new RemoteGossipMember(memberJSON.getString("host"),
		      memberJSON.getInt("port"), memberJSON.getString("host"));
		  settings.addGossipMember(member);
		  //configMembersDetails += member.getAddress();
		  //if (i < (membersJSON.length() - 1))
		  //  configMembersDetails += ", ";
		}
		//log.info( configMembersDetails + "]" );
	}
}
