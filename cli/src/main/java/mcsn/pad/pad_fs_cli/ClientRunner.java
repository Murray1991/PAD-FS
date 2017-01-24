package mcsn.pad.pad_fs_cli;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.*;

public class ClientRunner {
	
    public static void main( String[] args ) throws Exception {
    	System.out.println( "Hello World!" );

        Options options = new Options();
        
        Option interactive = new Option("i", "interactive", false, "interactive mode");
        options.addOption(interactive);
        
        Option config = new Option("c", "config", true, "configuration file in which are stored the ip addresses of the pad-fs nodes");
        options.addOption(config);
        
        Option dest = new Option("d", "dest", true, "specify the ip addr of the node where to send the requeste");
        options.addOption(dest);
        
        Option get = new Option("g", "get", true, "get value associated to a key from the pad-fs");
        options.addOption(get);
        
        Option put = new Option("p", "put", true, "put <key,value> or a file in the pad-fs");
        options.addOption(put);
        
        Option remove = new Option("r", "remove", true, "delete a key");
        options.addOption(remove);
        
        Option list = new Option("l", "list", false, "list all the keys for a node");
        options.addOption(list);
        
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = parser.parse(options, args);
        if (cmd == null) {
        	formatter.printHelp("utility-name", options);
            System.exit(1);
            return;  
        }

        String putV = cmd.getOptionValue("input");
        //String outputFilePath = cmd.getOptionValue("output");

        //System.out.println(inputFilePath);
        //System.out.println(outputFilePath);
        System.out.println(putV);
        
    }
}
