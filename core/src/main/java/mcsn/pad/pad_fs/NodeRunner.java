package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONException;

import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.storage.local.ActiveMapDBStore;
import mcsn.pad.pad_fs.storage.local.HashMapStore;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.storage.local.MapDBStore;

public class NodeRunner {

	public static void main(String[] args) throws FileNotFoundException, JSONException, IOException, InterruptedException {

		InputStream is = null;
		Properties props = new Properties();
		try {
			is = new FileInputStream("log4j.properties");
		    props.load(is);
		} catch (FileNotFoundException e) {
			System.out.println("log4j.properties not found");
		} finally {
			if (is != null) is.close();
		}
		
		PropertyConfigurator.configure(props);
		
		Options options = new Options();
		Option config = new Option("c", "config", true, "configuration file");
		Option bindAddr = new Option("b", "bindaddr", true, "binding address");
		Option path = new Option("p", "path", true, "the path in which store/retrieve the DBs");
		Option hmStore = new Option("h", "hmstore", false, "specifies to use the in-memory HashMap store");
		Option activeStore = new Option("a", "active", false, "specifies to use the ActiveMapDBStore store");
		Option help = new Option("help", "help", false, "help print");
		
		options.addOption(config);
		options.addOption(bindAddr);
		options.addOption(path);
		options.addOption(hmStore);
		options.addOption(activeStore);
		options.addOption(help);
		
		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
    	CommandLine cmd = null;
    	
    	try {
    		
    		cmd = parser.parse(options, args);
    		
    	} catch (ParseException e) {
    		System.out.println(e.getMessage());
    		formatter.printHelp("pad-fs", options);
    		System.exit(1);
    	}
    	
    	if (cmd.hasOption(help.getOpt())) {
    		formatter.printHelp("pad-fs", options);
    		System.exit(0);
    	}
    	
		File configFile = null;
		String addr = null;
		Path dirPath = null;
		Node padNode = null;
		
		if (cmd.hasOption(config.getOpt())) {
			configFile = new File(cmd.getOptionValue(config.getOpt()));
		} else {
			configFile = new File("./pad_fs.conf");
		}
		
		if (cmd.hasOption(bindAddr.getOpt())) {
			addr = cmd.getOptionValue(bindAddr.getOpt());
		}
		
		if (cmd.hasOption(path.getOpt())) {
			dirPath = Paths.get(cmd.getOptionValue(path.getOpt())+"/");
		} else {
			dirPath = Paths.get("./");
		}
		
		Class<? extends LocalStore> storeClass = MapDBStore.class;
		if (cmd.hasOption(hmStore.getOpt()))
			storeClass = HashMapStore.class;
		if (cmd.hasOption(activeStore.getOpt()))
			storeClass = ActiveMapDBStore.class;
		
		if (addr != null) {
			padNode = new Node(addr, configFile, dirPath, storeClass);
		} else {
			padNode = new Node(configFile, dirPath, storeClass);
		}

		System.out.println("-- start pad-fs node");
		System.out.println("-- press ^D (EOF) to shutdown");
		padNode.start();
		
		try {
	        byte[] b = new byte[1024];
	        while ( System.in.read(b) != -1 ) {
	        	String input = new String(b).trim();
	        	if (input.equals("members") || input.equals("m")) {
		     		for (Member m : padNode.getMemberList())
		        			System.out.println(m);
	        	} else if (input.equals("list") || input.equals("l")) { 
	        		List<Serializable> keys = padNode.getKeys();
	        		System.out.println("#keys: " + keys.size() );
	        		for (Serializable k : keys) {
	        				System.out.println(k);
	        		}
	        		
	        	} else {
	        		System.out.println("-- press ^D (EOF) to shutdown");
	        	}
	        }
	    } catch (Exception e) {
	    }
		
		System.out.println("-- shutdown pad-fs node");
		
		padNode.shutdown();
		
		System.exit(0);
	}
	
}
