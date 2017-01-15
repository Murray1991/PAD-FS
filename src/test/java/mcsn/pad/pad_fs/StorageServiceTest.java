package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

public class StorageServiceTest {
	
	private void initServices(List<IService> mServices, List<IService> sServices, List<LocalStore> lStores, int dim) throws FileNotFoundException, JSONException, IOException, InterruptedException {
		String filename = this.getClass().getResource("/gossip.conf").getFile();
		File configFile = new File(filename);
		for (int i = 1 ; i <= dim; i++) {
			Configuration config = new Configuration("127.0.0."+i, configFile);
			
			LocalStore localStore = new DummyLocalStore("127.0.0"+i);
			MembershipService membershipService = new MembershipService(config);
			
			lStores.add( localStore );
			mServices.add( membershipService );
			sServices.add( new StorageService(membershipService, localStore));
		}
	}
	
	private void startServices(List<IService> mServices, List<IService> sServices) {
		TestUtils.startServices(mServices);
		TestUtils.startServices(sServices);
	}
	
	private void shutdownServices(List<IService> mServices, List<IService> sServices) {
		TestUtils.shutdownServices(mServices);
		TestUtils.shutdownServices(sServices);
	}
	
	private Map<Serializable, ClientMessage> createMessages(int num) {
		Map<Serializable, ClientMessage> map = new HashMap<>();
		for (int i=0; i<num; i++) {
			ClientMessage msg = new ClientMessage(Message.PUT, TestUtils.getRandomString(), 
					TestUtils.getVersioned(TestUtils.getRandomString().getBytes()));
			map.put(msg.key, msg);
		}
		return map;
	}
	
	private void deliverMessages(Map<Serializable, ClientMessage> map, List<IService> sServices) {
		Iterator<Serializable> it = map.keySet().iterator();
		while (it.hasNext()) {
			ClientMessage msg = map.get(it.next());
			int idx = (Math.abs(msg.key.hashCode()) % sServices.size()) + 1;
			((StorageService) sServices.get(idx-1)).deliverMessage(msg);
		}
	}
	
	private void updateAndDeliverMessages(Map<Serializable, ClientMessage> map, List<IService> sServices) {
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
				int idx = (Math.abs(msg.key.hashCode()) % sServices.size()) + 1;
				((StorageService) sServices.get(idx-1)).deliverMessage(msg);
			}
		}
	}
	
	private void checkValues(Map<Serializable, ClientMessage> map, List<LocalStore> lStores){
		for (Serializable key : map.keySet() ) {
			int stores = TestUtils.countKey(key, lStores);
			
			Assert.assertTrue( stores == lStores.size() );
			
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
	}

	@Test
	public void testWithUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		List<IService> mServices = new ArrayList<>();
		List<IService> sServices = new ArrayList<>();
		List<LocalStore> lStores = new ArrayList<>();
		
		initServices(mServices, sServices, lStores, 4);
		startServices(mServices, sServices);
		
		System.out.println("-- Wait...");
		Thread.sleep(6000);
		
		Map<Serializable, ClientMessage> map = createMessages(300);
		
		System.out.println("-- Deliver Messages to the Services");
		deliverMessages(map, sServices);
		
		System.out.println("-- Wait...");
		Thread.sleep(20*1000);
		
		System.out.println("-- Update values");
		updateAndDeliverMessages(map, sServices);
		
		System.out.println("-- Wait...");
		Thread.sleep(20*1000);
		
		System.out.println("-- Shutdown all services");
		shutdownServices(mServices, sServices);
		
		checkValues(map, lStores);
		System.out.println("--- values are OK!");
		
	}
}
