package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
import mcsn.pad.pad_fs.message.client.GetMessage;
import mcsn.pad.pad_fs.message.client.ListMessage;
import mcsn.pad.pad_fs.message.client.PutMessage;
import mcsn.pad.pad_fs.storage.IStorageService;
import mcsn.pad.pad_fs.storage.StorageService;
import mcsn.pad.pad_fs.storage.local.HashMapStore;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.utils.TestUtils;
import voldemort.versioning.Versioned;

public class StorageServiceTest {
	
	private int dim = 4;
	private int num = 300;
	private List<IService> mServices;
	private List<IService> sServices;
	private List<LocalStore> lStores;
	Map<Serializable, ClientMessage> map;
	
	@Before
	public void setup() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		String filename = this.getClass().getResource("/gossip.conf").getFile();
		File configFile = new File(filename);
		mServices = new ArrayList<>();
		sServices = new ArrayList<>();
		lStores = new ArrayList<>();
		for (int i = 1 ; i <= dim; i++) {
			Configuration config = new Configuration("127.0.0."+i, configFile);
			MembershipService membershipService = new MembershipService(config);
			LocalStore localStore = new HashMapStore("127.0.0"+i);
			mServices.add( membershipService );
			lStores.add( localStore );
			sServices.add( new StorageService(membershipService, localStore));
		}
		TestUtils.startServices(mServices);
		TestUtils.startServices(sServices);
		System.out.println("-- wait for startup");
		Thread.sleep(6000);
	}
	
	@After
	public void teardown() {
		TestUtils.shutdownServices(mServices);
		TestUtils.shutdownServices(sServices);
		/* keys in map don't have multiple values in the system */
		TestUtils.checkValues(map, lStores);
	}
	
	@Test
	public void testWithoutUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		System.out.println("-- deliver messages to the system");
		map = TestUtils.createMessages(Message.PUT, TestUtils.createKeys(num));
		TestUtils.deliverMessages(map, sServices);
		System.out.println("-- wait");
		Thread.sleep(20000);
	}

	@Test
	public void testWithUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		testWithoutUpdates();
		System.out.println("-- update some values");
		updateAndDeliverMessages(map, sServices);
		System.out.println("-- wait");
		Thread.sleep(20000);
	}
	
	@Test
	public void testWithFailuresAndUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		testWithoutUpdates();
		System.out.println("-- shutdown member service of " + sServices.get(0)); 
		mServices.get(0).shutdown();
		IService removed = mServices.remove(0);
		System.out.println("-- wait");
		Thread.sleep(10000);
		
		/* each service here should know dim-2 members */
		TestUtils.checkIfCorrect(mServices, dim-2);
		System.out.println("-- update some values");
		updateAndDeliverMessages(map, sServices);
		System.out.println("-- wait");
		Thread.sleep(5000);
		
		System.out.println("-- restart " + sServices.get(0));
		removed.start();
		mServices.add(removed);
		System.out.println("-- wait");
		Thread.sleep(20000);
		TestUtils.checkIfCorrect(mServices, dim-1);
	}
	
	@Test
	public void concurrencyAndRemoveTest() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		
		testWithoutUpdates();
		
		System.out.println("-- shutdown all member services");
		TestUtils.shutdownServices(mServices);
		Thread.sleep(10000);
		
		/* each service should know nobody */
		TestUtils.checkIfCorrect(mServices, 0);
		
		/* different messages for same keys */
		List<Serializable> keys = TestUtils.createKeys(30);
		Map<Serializable, ClientMessage> map1 = TestUtils
				.createMessages(Message.PUT, keys);
		Map<Serializable, ClientMessage> map2 = TestUtils
				.createMessages(Message.PUT, keys);
		
		List<Map<Serializable, ClientMessage>> listMap = new ArrayList<>();
		listMap.add(map1);
		listMap.add(map2);
		
		System.out.println("-- put different values with same key in two different nodes");
		for (int i = 0; i < listMap.size(); i++) {
			StorageService ss = (StorageService) sServices.get(i);
			Map<Serializable, ClientMessage> map = listMap.get(i);
			deliverMessages(map, ss);
		}
		
		System.out.println("-- restart all the member services");
		TestUtils.startServices(mServices);
		
		/* wait for gossiping startup */
		System.out.println("-- wait");
		Thread.sleep(6000);
		
		/* each service should know dim-1 members */
		TestUtils.checkIfCorrect(mServices, dim-1);
		
		/* wait for convergence */
		System.out.println("-- wait");
		Thread.sleep(20000);
		
		System.out.println("-- get messages with multiple values");
		TestUtils.createMessages(Message.GET, keys)
		.forEach( (key, msg) -> {
			int idx = Math.abs(key.hashCode()) % dim;
			StorageService ss = (StorageService) sServices.get(idx);
			GetMessage res = (GetMessage) ss.deliverMessage(msg);
			Assert.assertTrue(res.status == Message.SUCCESS);
			Assert.assertNotNull(res.values);
			
			Assert.assertTrue("size: " + res.values.size(), res.values.size() == 2);
			
			listMap.forEach( map -> {
				PutMessage pmsg = (PutMessage) map.get(key);
				boolean match = res.values.stream()
						.anyMatch( vv -> vv.equals( pmsg.value ));
				Assert.assertTrue(match);
			});
		});
		
		/* select keys to remove */
		Random rand = new Random();
		keys.removeIf( k -> rand.nextInt(10) < 5);
		
		System.out.println("-- remove some keys");
		TestUtils.createMessages(Message.REMOVE, keys)
		.forEach( (key, msg) -> {	
			int idx = rand.nextInt(dim);
			StorageService ss = (StorageService) sServices.get(idx);
			ClientMessage res = ss.deliverMessage(msg);
			Assert.assertTrue(res.status == Message.SUCCESS);
		});
		
		Thread.sleep(2000);
		keys.forEach( key -> {
			boolean match = lStores.stream().allMatch( ls -> isEmpty( ls.get(key) ) );
			Assert.assertTrue(match);
		});
		
		System.out.println("-- try to get the removed keys");
		TestUtils.createMessages(Message.GET, keys)
		.forEach( (key, msg) -> {
			int idx = rand.nextInt(dim);
			StorageService ss = (StorageService) sServices.get(idx);
			ClientMessage res = ss.deliverMessage(msg);
			Assert.assertTrue(res.status == Message.NOT_FOUND);
		});

	}
	
	@Test
	public void removeTest() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		testWithoutUpdates();
		
		/* create the message to insert and then remove */
		String key = TestUtils.getRandomString();
		Map<Serializable, ClientMessage> pmap = TestUtils
				.createMessages(Message.PUT, Arrays.asList(key));
		Map<Serializable, ClientMessage> rmap = TestUtils
				.createMessages(Message.REMOVE, Arrays.asList(key));
		Map<Serializable, ClientMessage> gmap = TestUtils
				.createMessages(Message.GET, Arrays.asList(key));
		
		/* get coordinator for this key and the index of the service to stop*/
		Member c = ((MembershipService) mServices.get(0)).getCoordinator(key);
		System.out.println("Coordinator is " + c);
		
		int index;
		for ( index = 0 ; index < mServices.size(); index++ ) {
			Member m = ((MembershipService) mServices.get(index)).getMyself();
			if (m.equals(c)) break;
		}
		
		/* deliver and wait */
		ClientMessage msg = ((StorageService) sServices.get(0))
				.deliverMessage(pmap.get(key));
		Assert.assertTrue(msg.status == Message.SUCCESS);
		System.out.println("-- wait");
		Thread.sleep(10000);
		
		/* shutdown the membership service of the coordinator and wait*/
		IService ms = mServices.remove(index);
		ms.shutdown();
		
		System.out.println("-- wait");
		Thread.sleep(10000);
		
		TestUtils.checkIfCorrect(mServices, dim-2);
		
		/* remove the key when the coordinator is down*/
		msg = ((StorageService) sServices.get(0))
				.deliverMessage(rmap.get(key));
		Assert.assertTrue(msg.status == Message.SUCCESS);
		
		/* check if the key is correctly NOT FOUND in the system */
		msg = ((StorageService) sServices.get(0))
				.deliverMessage(gmap.get(key));
		Assert.assertTrue(msg.status == Message.NOT_FOUND);
		
		/* wait a little, restart membership service of coordinator, wait again */
		Thread.sleep(2000);
		mServices.add(index, ms);
		ms.start();
		Thread.sleep(10000);
		
		TestUtils.checkIfCorrect(mServices, dim-1);
		
		/* check if the key is correctly NOT FOUND in the system */
		msg = ((StorageService) sServices.get(0))
				.deliverMessage(gmap.get(key));
		Assert.assertTrue(msg.status == Message.NOT_FOUND);
	}

	@Test
	public void testList() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		
		testWithoutUpdates();
		
		System.out.println("-- send list message");
		ListMessage msg = new ListMessage();
		int idx = Math.abs(msg.hashCode()) % dim;
		
		StorageService ss = (StorageService) sServices.get(idx);
		ListMessage res = (ListMessage) ss.deliverMessage(msg);
		Assert.assertTrue(res.keys.size() == num);
	}
	
	private void deliverMessages(Map<Serializable, ClientMessage> map, IStorageService service) {
		for (Serializable key : map.keySet()) {
			ClientMessage res = service.deliverMessage(map.get(key));
			Assert.assertTrue(res.status == Message.SUCCESS);
		}
	}
	
	private void updateAndDeliverMessages(Map<Serializable, ClientMessage> map, List<IService> sServices) {
		Random rand = new Random();
		int bound = sServices.size();
		for (Serializable key : map.keySet()) {
			if (rand.nextInt(10) <= 3) {
				int idx = rand.nextInt(bound);
				ClientMessage msg = new PutMessage(key, TestUtils.getRandomString().getBytes());
				map.put(key, msg);
				StorageService ss = (StorageService) sServices.get(idx);
				ss.deliverMessage(msg);
			}
		}
	}
	
	private boolean isEmpty(List<Versioned<byte[]>> list) {
		return list == null || list.get(0).getValue() == null ;
	}
}
