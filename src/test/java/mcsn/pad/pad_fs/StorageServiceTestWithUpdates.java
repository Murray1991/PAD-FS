package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.json.JSONException;
import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Configuration;
import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.membership.MembershipService;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.storage.StorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.utils.DummyLocalStore;
import mcsn.pad.pad_fs.utils.TestUtils;

public class StorageServiceTestWithUpdates {

	@Test
	public void test() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		String filename = this.getClass().getResource("/gossip.conf").getFile();
		File configFile = new File(filename);
		
		int dim = 4;
		List<IService> mServices = new ArrayList<>();
		List<IService> sServices = new ArrayList<>();
		List<LocalStore> lStores = new ArrayList<>();
		for (int i = 1 ; i <= dim; i++) {
			Configuration config = new Configuration("127.0.0."+i, configFile);
			
			LocalStore localStore = new DummyLocalStore("127.0.0"+i);
			MembershipService membershipService = new MembershipService(config);
			
			lStores.add( localStore );
			mServices.add( membershipService );
			sServices.add( new StorageService(membershipService, localStore));
		}
		
		System.out.println("-- start services");
		TestUtils.startServices(mServices);
		TestUtils.startServices(sServices);
		
		System.out.println("-- wait few seconds");
		Thread.sleep(6000);
		
		int num = 300;
		System.out.println("-- build " + num + " messages and deliver them);");
		Map<Serializable, ClientMessage> map = new HashMap<>();
		for (int i=0; i<num; i++) {
			ClientMessage msg = new ClientMessage(Message.PUT, TestUtils.getRandomString(), 
					TestUtils.getVersioned(TestUtils.getRandomString().getBytes()));
			int idx = (Math.abs(msg.key.hashCode()) % dim) + 1;
			((StorageService) sServices.get(idx-1)).deliverMessage(msg);
			map.put(msg.key, msg);
		}
		
		int delta = 20;
		System.out.println("-- wait " + delta + " seconds");
		Thread.sleep(20*1000);
		
		for (Serializable key : map.keySet() ) {
			Assert.assertTrue( TestUtils.existKey(key, lStores) );
		}
		System.out.println("--- keys exist!");
		
		System.out.println("-- update values");
		Random rand = new Random();
		for (Serializable key : map.keySet()) {
			if (rand.nextInt(10) <= 3) {
				ClientMessage msg = 
						new ClientMessage(
							Message.PUT, 
							key, 
							TestUtils.getVersioned(
									TestUtils.getRandomString().getBytes()
									)
						);
				map.put(key, msg);
				int idx = (Math.abs(msg.key.hashCode()) % dim) + 1;
				((StorageService) sServices.get(idx-1)).deliverMessage(msg);
			}
		}
		
		System.out.println("-- wait " + delta + " seconds");
		Thread.sleep(20*1000);
		
		System.out.println("-- shutdown services");
		TestUtils.shutdownServices(mServices);
		TestUtils.shutdownServices(sServices);
		
		for (Serializable key : map.keySet() ) {
			int stores = TestUtils.countKey(key, lStores);
			
			Assert.assertTrue( stores == dim );
			
			Assert.assertTrue( 
					
					"[" + new String(lStores.get(0).get(key).getValue()).substring(0, 5) + " -- " + lStores.get(0).get(key).getVersion() + "] ;; " + 
							"[" + new String(lStores.get(1).get(key).getValue()).substring(0, 5) + lStores.get(1).get(key).getVersion()  + "] ;;"  +
							"[" + new String(lStores.get(2).get(key).getValue()).substring(0, 5) + lStores.get(2).get(key).getVersion()  + "] ;; " +
							"[" + new String(lStores.get(3).get(key).getValue()).substring(0, 5) + lStores.get(3).get(key).getVersion()  + "] ;;" +
							"[" + new String(map.get(key).value.getValue()).substring(0, 5) + map.get(key).value.getVersion() + "] ;;",
							
					TestUtils.checkValues( 
							TestUtils.getValues(key, lStores), 
							lStores.get(0).get(key)
							) 
					
					); 
			
			Assert.assertTrue( 
					TestUtils.checkValues( 
							TestUtils.getValues(key, lStores), 
							map.get(key).value
							) 
					); 
		}
		
		System.out.println("--- values are OK!");
		
	}

}
