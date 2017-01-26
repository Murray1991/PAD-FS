package mcsn.pad.pad_fs_cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.cli.ParseException;

import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;

public class ClientRunner {
	
    public static void main( String[] args ) throws Exception {
    	
    	Random rand = new Random();
    	OptionManager om = new OptionManager();

    	try {
			om.parse(args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
        	om.printHelp("pad-fs-cli");
        	System.exit(1);
        	return;
		}
    	
    	int type = 0;
        String[] values = null;
    	ArrayList<InetSocketAddress> addresses = om.getAddressPool();

    	for (boolean run = true; run; ) {
    		if (om.isInteractive()) {
        		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            	String line = br.readLine();
            	if (line.equals("q") || line.equals("quit"))
            		break;
            	
            	String[] parts = line.split("\\W+", 2);
            	type = om.getType(parts[0]);
            	values = parts[1].split("\\W+");
            	br.close();
        	} 
        	
        	if (! om.isInteractive()){
        		type = om.getType();
        		values = om.getValues();
        		run = false;
        	}
        	
        	ClientMessage rcvMsg = null;
        	int size = addresses.size();
        	int idx = rand.nextInt(size);
        	
        	for (int i = 0; i < addresses.size() && rcvMsg == null; i++ ) {
        		try {
        			InetSocketAddress raddr = addresses.get( (idx+i) % size );
        			rcvMsg = new Client(raddr).send(type, values);
                	processMessage(rcvMsg, om.getOutputPathFile());
                } catch (IOException | ClassNotFoundException e){
    			}
        	}
    	}
    }

	private static void processMessage(ClientMessage msg, String outputPathFile) {
		
		if (msg.status == Message.ERROR) {
        	System.err.println("Error: remote failure, please retry");
        }
		
        if (msg.status == Message.NOT_FOUND) {
        	System.out.println("Your request is not present in the pad-fs system");
        }
        
        byte[] data = null;
        if (msg.type == Message.GET && msg.status == Message.SUCCESS ) {
        	data = msg.value.getValue();
        }
        
        try {
        	
	        if (data != null && outputPathFile != null) {
	        	System.out.println("saving in \"" + outputPathFile + "\"");
	        	new File(outputPathFile).createNewFile();
	        	Files.write(Paths.get(outputPathFile), data);
	        }
	        
	        if (data != null && outputPathFile == null) {
	        	System.out.println(new String(data));
	        }
	        
        } catch (IOException e) {
			System.err.println("Impossible saving in " + outputPathFile);
		}
		
	}
}
