package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.JSONException;

import mcsn.pad.pad_fs.storage.local.MapDBStore;

public class NodeRunner {

	public static void main(String[] args) throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		Options options = new Options();
		Option config = new Option("c", "config", true, "configuration file");
		Option bindAddr = new Option("b", "bindaddr", true, "binding address");
		Option path = new Option("p", "path", true, "the path in which store/retrieve the DBs");
		
		options.addOption(config);
		options.addOption(bindAddr);
		options.addOption(path);
		
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
		
		if (addr != null) {
			padNode = new Node(addr, configFile, dirPath, MapDBStore.class);
		} else {
			padNode = new Node(configFile, dirPath, MapDBStore.class);
		}

		System.out.println("-- start pad-fs node");
		System.out.println("-- press ^D (EOF) to shutdown");
		padNode.start();
		
		try {
	        byte[] b = new byte[1024];
	        while ( System.in.read(b) != -1 ) {
	            System.out.println("-- press ^D (EOF) to shutdown");
	        }
	    } catch (Exception e) {
	    }
		
		System.out.println("-- shutdown pad-fs node");
		padNode.shutdown();
		
		System.exit(0);
	}
	
}
