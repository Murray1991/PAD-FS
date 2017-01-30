package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Configuration;
import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.membership.IMembershipService;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.membership.MembershipService;
import mcsn.pad.pad_fs.utils.TestUtils;

public class MembershipServiceTest {
	
	private List<IService> services;
	private final int dim = 4;
	
	@Before
	public void setup() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		System.out.println("-- setup MembershipServiceTest");
		this.services = new ArrayList<>(dim);
		String filename = this.getClass().getResource("/gossip.conf").getFile();
		File configFile = new File(filename);
		for (int i = 1 ; i <= dim; i++) {
			Configuration config = new Configuration("127.0.0."+i, configFile);
			services.add(new MembershipService(config));
		}
		
		TestUtils.startServices(services);
		Thread.sleep(10000);
	}
	
	@After
	public void teardown() {
		System.out.println("-- teardown MembershipServiceTest");
		TestUtils.shutdownServices(services);
		services = null;
	}

	//@Test
	public void gossipTest() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		// Each member should know dim-1 friends
		checkIfCorrect(services, dim-1);
	}
	
	//@Test
	public void gossipTestWithFailures() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		// Each member should know dim-1 friends
		checkIfCorrect(services, dim-1);
		
		//Shutdown a given service
		IService removed0 = services.remove(0);
		System.out.println("-- shutdown " + ((MembershipService) removed0).getMyself());
		removed0.shutdown();

		//Shutdown another service
		IService removed1 = services.remove(0);
		System.out.println("-- shutdown " + ((MembershipService) removed1).getMyself());
		removed1.shutdown();
		
		Assert.assertTrue(! removed0.isRunning() );
		Assert.assertTrue(! removed1.isRunning() );
		Thread.sleep(20000);
		
		//Each member should know dim-3 friends
		System.out.println("-- check failure detection");
		//TODO sometimes, problem here with two failures...
		checkIfCorrect(services, dim-3);
		
		//Reinsert the services in the list and restart
		System.out.println("-- restart " + ((MembershipService) removed0).getMyself());
		removed0.start();
		services.add(removed0);
		System.out.println("-- restart " + ((MembershipService) removed1).getMyself());
		removed1.start();
		services.add(removed1);
		
		Thread.sleep(8000);
		
		//Check if members are restored
		checkIfCorrect(services, dim-1);
	}
	
	@Test
	public void coordinatorTest() throws FileNotFoundException, JSONException, IOException, InterruptedException {
	
		String key = TestUtils.getRandomString();
		
		//Each member should know dim-1 friends
		checkIfCorrect(services, dim-1);
		
		//Get the coordinator for the key and shutdown it
		Member m1 = getCoordinator(services, key);
		IService removed = findServiceForMember(services, m1);
		services.remove(removed);
		removed.shutdown();
		
		//An immediate getCoordinator call should give the same coordinator
		Assert.assertTrue( m1.equals( getCoordinator(services, key) ) );
		
		Thread.sleep(10000);
	
		//After the cleanup_interval each "alive" member should know dim-2 firends
		checkIfCorrect(services, dim-2);
		
		//Here getCoordinator should give a different coordinator
		Assert.assertTrue( ! m1.equals( getCoordinator(services, key) ) );
		
		//The coordinator of any key for the removed member should be the member itself
		Assert.assertTrue( m1.equals( ((MembershipService) removed).getCoordinator(key) ));
		for (int i = 0; i < 10; i++ ) {
			String anotherKey = TestUtils.getRandomString();
			Assert.assertTrue( m1.equals( ((MembershipService) removed).getCoordinator(anotherKey) ));
		}
		
		//Reinsert the service in the list and restart
		services.add(removed);
		removed.start();
		
		Thread.sleep(8000);
		
		//Check the same conditions at the beginning after some time
		checkIfCorrect(services, dim-1);
		Assert.assertTrue(m1.equals( getCoordinator(services, key) ));
	}
	
	private void checkIfCorrect(List<IService> services, int expected) {
		for (IService service : services) {
			MembershipService ms = (MembershipService) service;
			Assert.assertTrue(printInfo(ms.getMyself(), ms.getMembers()), ms.getMembers().size() == expected);
		}
	}
	
	private String printInfo(Member me , List<Member> members) {
		String s = "Myself: " + me.toString();
		for (Member m : members) {
			s += ";; " + m.toString();
		}
		return s;
	}
	
	private Member getCoordinator(List<IService> services, String key) {
		MembershipService msp = (MembershipService) services.get(0);
		Member p = msp.getCoordinator(key);
		List<Member> mspMembers = msp.getMembers();
		for (IService service : services) {
			MembershipService msm = (MembershipService) service;
			Member m = msm.getCoordinator(key);
			Assert.assertNotNull(p);
			Assert.assertNotNull(m);
			List<Member> msmMembers = msm.getMembers();
			Assert.assertTrue(printInfo(p, mspMembers) + printInfo(m, msmMembers), 
					p.equals(m));
		}
		return p;
	}
	
	private IService findServiceForMember(List<IService> services, Member member) {
		for (IService service : services) {
			if (((IMembershipService) service).getMyself().equals(member))
				return service;
		}
		return null;
	}
}
