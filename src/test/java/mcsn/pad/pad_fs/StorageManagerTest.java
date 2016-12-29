package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Configuration;
import mcsn.pad.pad_fs.membership.MembershipService;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.storage.StorageService;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.utils.DummyLocalStore;
import mcsn.pad.pad_fs.utils.TestUtils;

public class StorageManagerTest {

	@Test
	public void test() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		
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
		List<Message> messages = TestUtils.getMessages(Message.PUT, num);
		for ( Message msg : messages) {
			int idx = (Math.abs(msg.key.hashCode()) % dim) + 1;
			System.out.println("--- Message: " + msg + " to storageService-" + idx);
			sServices.get(idx-1).deliverMessage(msg);
		}
		
		System.out.println("Shutdown the services...");
		for ( StorageService ss : sServices ) {
			ss.shutdown();
		}
		
		for ( MembershipService ms : mServices ) {
			ms.shutdown();
		}
		
		System.out.println("Begin assertions...");
		for (Message m : messages ) {
			boolean b = false;
			for ( LocalStore l : lStores ) {
				b = b || l.get(m.key) != null;
			}
			Assert.assertTrue(b);
		}

		System.out.println("--- All OK");
	}

}
