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
import java.util.Random;
import java.util.Map.Entry;

import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Configuration;
import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.membership.MembershipService;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.storage.StorageService;
import mcsn.pad.pad_fs.storage.local.HashMapStore;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.utils.TestUtils;

public class ConcurrencyTest {

	private int dim = 5;
	private int num = 300;
	private List<IService> mServices;
	private List<IService> sServices;
	private List<LocalStore> lStores;
	
	private MembershipService mBridge;
	private StorageService sBridge;
	private LocalStore lBridge;
	
	Map<Serializable, ClientMessage> map;
	
	@Before
	public void setup() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		String filename12345 = this.getClass().getResource("/pad_fs.conf").getFile();
		String filename123 = this.getClass().getResource("/pad_fs1.conf").getFile();
		String filename345 = this.getClass().getResource("/pad_fs2.conf").getFile();
		
		File configFile12 = new File(filename123);
		File configFile45 = new File(filename345);
		File configFile3 = new File(filename12345);
		
		mServices = new ArrayList<>();
		sServices = new ArrayList<>();
		lStores = new ArrayList<>();
		
		for (int i = 1 ; i <= 2; i++) {
			Configuration config = new Configuration("127.0.0."+i, configFile12);
			MembershipService membershipService = new MembershipService(config);
			LocalStore localStore = new HashMapStore("127.0.0"+i);
			mServices.add( membershipService );
			lStores.add( localStore );
			sServices.add( new StorageService(membershipService, localStore));
		}
		
		for (int i = 4 ; i <= 5; i++) {
			Configuration config = new Configuration("127.0.0."+i, configFile45);
			MembershipService membershipService = new MembershipService(config);
			LocalStore localStore = new HashMapStore("127.0.0"+i);
			mServices.add( membershipService );
			lStores.add( localStore );
			sServices.add( new StorageService(membershipService, localStore));
		}
		
		//"bridge" configuration
		Configuration config = new Configuration("127.0.0.3", configFile3);
		mBridge = new MembershipService(config);
		lBridge = new HashMapStore("127.0.0.3");
		sBridge = new StorageService(mBridge, lBridge);
		
		TestUtils.startServices(mServices);
		TestUtils.startServices(sServices);
		
		map = createMessages(Message.PUT, createKeys(num));
		Thread.sleep(8000);
	}
	
	@After
	public void teardown() {
		TestUtils.shutdownServices(mServices);
		TestUtils.shutdownServices(sServices);
		mBridge.shutdown();
		sBridge.shutdown();
		TestUtils.checkValues(map, lStores);
	}
	
	void printMembers(MembershipService ms) {
		System.out.println("Members of " + ms.getMyself().host + ":");
		for (Member m : ms.getMembers())
			System.out.println(m.host);
	}
	
	@Test
	public void test() throws InterruptedException {
		
		printMembers((MembershipService) mServices.get(0));
		printMembers((MembershipService) mServices.get(2));
		
		System.out.println("-- deliver messages at random among the sServices");
		deliverMessages(map, sServices);
		System.out.println("-- done");
		Thread.sleep(6000);
		
		/* two "islands" [1,2] and [4,5] are present */
		checkIfCorrect(mServices, 1);
		
		System.out.println("-- create keys");
		List<Serializable> keys = createKeys(30);
		List<Map<Serializable, ClientMessage>> list = new ArrayList<>();
		Random rand = new Random();
		
		System.out.println("-- ship messages with same key but different values to two different islands");
		for (int i=0; i<2; i++) {
			Map<Serializable, ClientMessage> msgs = 
					createMessages(Message.PUT, keys);
			int idx = i == 0 ? 0 : 2;
			for (Serializable key : msgs.keySet()) {
				StorageService ss = (StorageService) sServices.get(idx);
				ClientMessage res = ss.deliverMessage(msgs.get(key));
				Assert.assertTrue(res.status == Message.SUCCESS);
			}
			list.add(msgs);
		}
		Thread.sleep(2000);
		
		System.out.println("-- start the bridge node 3");
		mBridge.start();
		sBridge.start();
		
		System.out.println("-- wait...");
		Thread.sleep(20000);
		
		checkIfCorrect(mServices, dim-1);
		Map<Serializable, ClientMessage> getMessages =
				createMessages(Message.GET, keys);
		
		System.out.println("-- concurrency test...");
		for (int i=0; i<2; i++) {
			for (Entry<Serializable, ClientMessage> e : getMessages.entrySet()) {
				int idx = rand.nextInt(dim-1);
				StorageService ss = (StorageService) sServices.get(idx);
				ClientMessage res = ss.deliverMessage(e.getValue());	
				Assert.assertTrue("type: " + res.status, res.status == Message.SUCCESS);
				Assert.assertNotNull(res.values);
				
				byte[] exp = list.get(i).get(e.getKey()).value.getValue();
				byte[] res1 = res.values.get(0).getValue();
				byte[] res2 = res.values.get(1).getValue();
				Assert.assertTrue( "something is wrong... ",
						Arrays.equals(exp, res1) || Arrays.equals(exp, res2));
			}
		}
	}
	
	private void checkIfCorrect(List<IService> services, int expected) {
		for (IService service : services) {
			MembershipService ms = (MembershipService) service;
			Assert.assertTrue(ms.getMembers().size() == expected);
		}
	}
	
	private void deliverMessages(Map<Serializable, ClientMessage> map, List<IService> sServices) {
		Iterator<Serializable> it = map.keySet().iterator();
		while (it.hasNext()) {
			ClientMessage msg = map.get(it.next());
			int idx = (Math.abs(msg.key.hashCode()) % sServices.size()) + 1;
			StorageService ss = (StorageService) sServices.get(idx-1);

			ClientMessage rcvMsg = ss.deliverMessage(msg);
			Assert.assertTrue("status type: " + rcvMsg.status, rcvMsg.status == Message.SUCCESS);
		}
	}
	
	private Map<Serializable, ClientMessage> createMessages(int type, List<Serializable> keys) {
		Map<Serializable, ClientMessage> map = new HashMap<>();
		for (Serializable key : keys)
			switch (type) {
			case Message.PUT:
				map.put(key, new ClientMessage(type, key, 
						TestUtils.getVersioned(TestUtils.getRandomString().getBytes())));
				break;
			case Message.GET:
			case Message.REMOVE:
				map.put(key, new ClientMessage(type, key, true));
			default:
				break;
			}
		return map;
	}
	
	private List<Serializable> createKeys(int num) {
		List<Serializable> keys = new ArrayList<>();
		for (int i=0; i<num; i++)
			keys.add(TestUtils.getRandomString());
		return keys;
	}

}
