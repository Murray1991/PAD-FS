package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.json.JSONException;

import mcsn.pad.pad_fs.storage.local.MapDBStore;

public class NodeRunner {

	public static void main(String[] args) throws FileNotFoundException, JSONException, IOException, InterruptedException {
		File configFile = null;
		Node padNode = null;
		
		if (args.length == 0) {
			configFile = new File("./pad_fs.conf");
		} else if (args.length >= 1) {
			configFile = new File("./"+args[0]);
		}
		
		System.out.println("Active Count: " + Thread.currentThread().getThreadGroup().activeCount());
		if (args.length >= 2) {
			padNode = new Node(args[1], configFile, MapDBStore.class);
		} else {
			padNode = new Node(configFile, MapDBStore.class);
		}
		
		System.out.println("Active Count: " + Thread.currentThread().getThreadGroup().activeCount());
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
	
		ThreadGroup tg = Thread.currentThread().getThreadGroup();
		System.out.println(tg.activeCount());
	}
	
}
