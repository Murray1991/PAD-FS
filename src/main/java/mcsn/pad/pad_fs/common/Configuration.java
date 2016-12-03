package mcsn.pad.pad_fs.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.json.JSONException;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LogLevel;
import com.google.code.gossip.RemoteGossipMember;
import com.google.code.gossip.StartupSettings;

public class Configuration {
	
	private String ipAddress;

	private StartupSettings settings;
	
	public Configuration(String ipAddress, StartupSettings settings) {
		this.ipAddress = ipAddress;
		this.settings = settings;
	}
	
	public Configuration(String ipAddress, File configFile) throws FileNotFoundException, JSONException, IOException {
		this (ipAddress, StartupSettings.fromJSONFile(configFile));
	}
	
	public Configuration(File configFile) throws UnknownHostException, FileNotFoundException, JSONException, IOException {
		this (InetAddress.getLocalHost().getHostAddress(), StartupSettings.fromJSONFile(configFile));
	}
	
	public Configuration(String seed, int port) throws UnknownHostException {
		this.settings = new StartupSettings(port, LogLevel.DEBUG);
		this.ipAddress = InetAddress.getLocalHost().getHostAddress();
		
		settings.addGossipMember(new RemoteGossipMember(seed, port, ""));
	}
	
	public String getHost() {
		return ipAddress;
	}
	
	public int getPort() {
		return settings.getPort();
	}
	
	public int getLogLevel() {
		return settings.getLogLevel();
	}
	
	public List<GossipMember> getGossipMembers() {
		return settings.getGossipMembers();
		
	}
	
	public GossipSettings getGossipSettings() {
		return settings.getGossipSettings();
	}
}
