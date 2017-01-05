package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.json.JSONException;
import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Configuration;
import mcsn.pad.pad_fs.membership.MembershipService;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.storage.StorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.utils.DummyLocalStore;
import mcsn.pad.pad_fs.utils.TestUtils;
import voldemort.versioning.Versioned;

public class StorageServiceTest {

	@Test
	public void test() throws FileNotFoundException, JSONException, IOException, InterruptedException {

		String filename = this.getClass().getResource("/gossip.conf").getFile();
		File configFile = new File(filename);
		
		int dim = 4;
		List<MembershipService> mServices = new ArrayList<>();
		List<StorageService> sServices = new ArrayList<>();
		List<LocalStore> lStores = new ArrayList<>();
		for (int i = 1 ; i <= dim; i++) {
			Configuration config = new Configuration("127.0.0."+i, configFile);
			MembershipService membershipService = new MembershipService(config);
			LocalStore localStore = new DummyLocalStore("127.0.0"+i);
			
			mServices.add( membershipService );
			lStores.add( localStore );
			sServices.add( new StorageService(membershipService, localStore));
		}
		
		System.out.println("Start the services...");
		for ( MembershipService ms : mServices )
			ms.start();
		
		for ( StorageService ss : sServices )
			ss.start();
		
		System.out.println("Wait few seconds...");
		Thread.sleep(6000);
		
		int num = 500;
		System.out.println("Build " + num + " messages and deliver them");
		List<ClientMessage> messages = TestUtils.getMessages(Message.PUT, num);
		Map<Serializable, ClientMessage> hmessages = new HashMap<>();
		
		for ( ClientMessage msg : messages) {
			int idx = (Math.abs(msg.key.hashCode()) % dim) + 1;
			hmessages.put(msg.key, msg);
			System.out.println("--- ClientMessage: " + msg + " to storageService-" + idx);
			sServices.get(idx-1).deliverMessage(msg);
		}
		
		System.out.println("Begin assertions..." );
		for (ClientMessage m : messages ) {
			Assert.assertTrue( existKey(m.key, lStores) );
		}
		System.out.println("--- keys exist");
		
		int delta =  20000; //20 sec
		System.out.println("Wait " + delta/1000 + " seconds...");
		Thread.sleep(delta);
		
		System.out.println("Shutdown the services...");
		for ( StorageService ss : sServices ) {
			ss.shutdown();
		}
		
		for ( MembershipService ms : mServices ) {
			ms.shutdown();
		}
		
		int i = 0;
		for (ClientMessage m : messages ) {
			int stores = countKey(m.key, lStores);
			Assert.assertTrue( "stores: " + stores + " , dim: " + dim + " , " + i, stores == dim );
			Assert.assertTrue(  
					error(i++, m.key, hmessages.get(m.key).value, getValues(m.key, lStores)), 
					equalValues(getValues(m.key, lStores), hmessages.get(m.key).value) );
			i++;
		}
		
		System.out.println("--- key replicated in all the stores");
		
	}
	
	public String error (int i, Serializable key, Versioned<byte[]> original, Iterable<Versioned<byte[]>> values) {
		String str = "";
		if (original.getValue() == null)
			str += "oh my god original.getValue returns null" ;
		
		System.out.println("Original value: " + new String(original.getValue()));
		for (Versioned<byte[]> value : values) {
			if (value.getValue() == null)
				str += "oh my god value.getValue returns null";
			else str += "; Replicated Value: " + new String(value.getValue());
		}
		str += "; Error at " + i + " for key " + key;
		return str;
	}
	
	public int countKey (Serializable key, List<LocalStore> lStores) {
		int count = 0;
		for ( LocalStore l : lStores )
			count  += (l.get(key) != null ? 1 : 0);
		return count;
	}
	
	public Iterable<Versioned<byte[]>> getValues (Serializable key, List<LocalStore> lStores) {
		Vector<Versioned<byte[]>> values = new Vector<Versioned<byte[]>>();
		for ( LocalStore l : lStores )
			values.add(l.get(key));
		return values;
	}
	
	public boolean equalValues (Iterable<Versioned<byte[]>> values, Versioned<byte[]> original) {
		Iterator<Versioned<byte[]>> itValue = values.iterator();
		while (itValue.hasNext()) {
			Versioned<byte[]> curr = itValue.next();
			if ( ! Arrays.equals(original.getValue(), curr.getValue()) ) {
				System.out.println("Value different, wtf");
				return false;
			}
			// THIS IS NOT HOW IT WORKS
			/* if ( original.getVersion().compare(curr.getVersion()) != Occurred.CONCURRENTLY ) {
				System.out.println("Versions different, why?");
				return false; 
			}*/
		}
		return true;
	}
	
	public boolean existKey (Serializable key, List<LocalStore> lStores) {
		return countKey (key, lStores) != 0;
	}

}
