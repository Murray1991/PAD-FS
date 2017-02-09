package mcsn.pad.pad_fs_cli;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import mcsn.pad.pad_fs.message.Message;

public class OptionManager {
	
	final private String defaultConfigPath = "./nodes.conf";
	private Option interactive;
	private Option config;
	private Option dest;
	private Option get;
	private Option put;
	private Option remove;
	private Option list;
	private Option out;
	
	private HashMap<Integer,Integer> hm;
	private Options options;
	private boolean inter;
	private Option op;
	private CommandLine cmd;
	private ArrayList<InetSocketAddress> addresses;

	public OptionManager() {
		
		options = new Options();
        
        interactive = new Option("i", "interactive", false, "interactive mode");
        config = new Option("c", "config", true, "configuration file in which are stored the ip addresses of the pad-fs nodes");
        dest = new Option("d", "dest", true, "specify the ip addr of the node where to send the requeste");
        get = new Option("g", "get", true, "get value associated to a key from the pad-fs");
        put = new Option("p", "put", true, "put <key,value> or a file in the pad-fs");
        remove = new Option("r", "remove", true, "delete a key");
        list = new Option("l", "list", false, "list all the keys for a node");
        out = new Option("o", "output", true, "store in a file the response for a GET");
        
        //key [value]
        put.setArgs(Option.UNLIMITED_VALUES);
        
        options.addOption(interactive);
        options.addOption(config);
        options.addOption(get);
        options.addOption(dest);
        options.addOption(put);
        options.addOption(remove);
        options.addOption(list);
        options.addOption(out);
        
        hm = new HashMap<>();
        hm.put(get.getId(), Message.GET);
        hm.put(put.getId(), Message.PUT);
        hm.put(remove.getId(), Message.REMOVE);
        hm.put(list.getId(), Message.LIST);
	}
	
	/** 
	 * This method MUST ALWAYS be called immediately after the constructor
	 * Bad practice, I know, but it's useful...
	 * */
	public void parse(String[] args) throws ParseException {
		CommandLineParser parser = new DefaultParser();
        addresses = null;
    	cmd = parser.parse(options, args);
    	
    	if (args.length == 0 || cmd == null) {
    		throw new ParseException("No arguments have been provided");
    	}
    	
    	/* get the pad-fs addresses from the configuration file or the dest option */
    	if (!cmd.hasOption(dest.getOpt())) {
    		String path = cmd.hasOption( config.getOpt() ) ? 
    				cmd.getOptionValue(config.getOpt()) : defaultConfigPath;
    		addresses = getAddressesFromFile(path);
    	} else {
    		String[] addr_port = cmd.getOptionValues(dest.getOpt());
    		String addr = addr_port[0];
    		int port = addr_port.length > 1 ? Integer.parseInt(addr_port[1]) : 8080;
    		addresses = new ArrayList<InetSocketAddress>();
    		addresses.add(new InetSocketAddress(addr, port));
    	}
    	
    	Object[] ops = Arrays.stream( cmd.getOptions() ).filter( o -> {
        	return o.equals(put) || o.equals(get) 
        			|| o.equals(remove) || o.equals(list);
        }).toArray();
    	
    	long n = ops.length;
    	inter = cmd.hasOption(interactive.getOpt()); 
        if (n == 0 && !inter) {
        	throw new ParseException("No operations have been provided");
        }
        if (n > 1 && !inter) {
        	throw new ParseException("Too many operations have been provided");
        }
        if (n > 0 && inter) {
        	throw new ParseException("Operation and interactive mode can't exist together");
        }
        op = (Option) ((n == 1 && !inter) ? ops[0] : null);
        if (n == 1 && op.equals(put) && getValues().length > 2) {
        	throw new ParseException("Too many arguments for p");
        }
	}
	
	public int getType(String opt) {
		return hm.get(options.getOption(opt).getId());
	}
	
	/**
	 * This succeeds only iff isInteractive() == false
	 * Otherwise returns a RuntimeException 
	 * @throws RuntimeException 
	 * */
	public int getType() {
		if (op == null) {
			throw new RuntimeException("don't call this when interactive mode is enabled");
		}
		
		return hm.get(op.getId());
	}
	
	/**
	 * This succeeds only iff isInteractive() == false
	 * Otherwise returns a RuntimeException 
	 * @throws RuntimeException 
	 * */
	public String[] getValues() throws RuntimeException {
		if (op == null) {
			throw new RuntimeException("don't call this when interactive mode is enabled");
		}
		
		return cmd.getOptionValues(op.getOpt());
	}
	
	public boolean isInteractive() {
		return inter;
	}
	
	public ArrayList<InetSocketAddress> getAddressPool() {
		return addresses;
	}
	
	public void printHelp(String program) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(program, options);
	}
	
	public String getOutputPathFile() {
		return cmd.getOptionValue(out.getOpt());
	}
	
	private ArrayList<InetSocketAddress> getAddressesFromFile(String path) throws ParseException {

		StringBuffer buffer = null;
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
			buffer = new StringBuffer();
		    String line;
		    while ((line = br.readLine()) != null) {
		      buffer.append(line.trim());
		    }
		    br.close();
		} catch (FileNotFoundException e) {
			throw new ParseException("File \"" + path + "\" does not exist");
		} catch (IOException e) {
			throw new ParseException("IO exception in parsing the file");
		}
	    
		ArrayList<InetSocketAddress> addresses = new ArrayList<>();
		try {
			JSONObject jsonObj = new JSONArray(buffer.toString()).getJSONObject(0);
			JSONArray nodesJSON = jsonObj.getJSONArray("nodes");
		    for (int i=0; i<nodesJSON.length(); i++) {
		    	JSONObject node = nodesJSON.getJSONObject(i);
		    	addresses.add(new InetSocketAddress(node.getString("host"), node.getInt("port")));
		    }
		} catch (JSONException e) {
			throw new ParseException("Bad JSON format");
		}
		
		return addresses;
	}
}
