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
		TestUtils.shutdownServices(services);
	}

	@Test
	public void gossipTest() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		/* each member should know dim-1 friends */
		TestUtils.checkIfCorrect(services, dim-1);
	}
	
	@Test
	public void gossipTestWithFailures() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		/* each member should know dim-1 friends */
		TestUtils.checkIfCorrect(services, dim-1);
		
		/* shutdown the first service */
		IService removed0 = services.remove(0);
		removed0.shutdown();

		/* shutdown the second service */
		IService removed1 = services.remove(0);
		removed1.shutdown();
		
		Assert.assertFalse( removed0.isRunning() );
		Assert.assertFalse( removed1.isRunning() );
		System.out.println("-- wait");
		Thread.sleep(10000);
		
		/* each left member should know dim-3 friends */
		TestUtils.checkIfCorrect(services, dim-3);
		
		/* reinsert the services in the list and restart */
		removed0.start();
		removed1.start();
		services.add(removed0);
		services.add(removed1);
		
		System.out.println("-- wait");
		Thread.sleep(8000);
		
		/* check if members are restored */
		TestUtils.checkIfCorrect(services, dim-1);
	}
	
	@Test
	public void coordinatorTest() throws FileNotFoundException, JSONException, IOException, InterruptedException {
	
		String key = TestUtils.getRandomString();
		
		TestUtils.checkIfCorrect(services, dim-1);
		
		/* get the coordinator for the key and shutdown it */
		Member m1 = getCoordinator(services, key);
		IService removed = (IService) services
				.stream()
				.filter( m -> ((IMembershipService) m).getMyself().equals(m1) )
				.toArray()[0];
		services.remove(removed);
		removed.shutdown();
		
		/* an immediate getCoordinator call should give the same coordinator */
		Assert.assertTrue( m1.equals( getCoordinator(services, key) ) );
		
		Thread.sleep(10000);
	
		/* after the cleanup_interval each "alive" member should know dim-2 firends */
		TestUtils.checkIfCorrect(services, dim-2);
		
		/* here getCoordinator should give a different coordinator */
		Assert.assertTrue( ! m1.equals( getCoordinator(services, key) ) );
		
		/* the coordinator of any key for the removed member should be the member itself */
		Member expected = ((MembershipService) removed).getCoordinator(key);
		Assert.assertTrue( m1.equals(expected) );
		for (int i = 0; i < 10; i++ ) {
			String anotherKey = TestUtils.getRandomString();
			expected = ((MembershipService) removed)
					.getCoordinator(anotherKey);
			Assert.assertTrue( m1.equals(expected) );
		}
		
		/* reinsert the service in the list and restart */
		services.add(removed);
		removed.start();
		
		System.out.println("-- wait");
		Thread.sleep(8000);
		
		/* check the same conditions at the beginning after some time */
		TestUtils.checkIfCorrect(services, dim-1);
		Assert.assertTrue(m1.equals( getCoordinator(services, key) ));
	}
	
	private Member getCoordinator(List<IService> services, String key) {
		MembershipService msp = (MembershipService) services.get(0);
		Member p = msp.getCoordinator(key);
		Assert.assertNotNull(p);
		List<Member> mspMembers = msp.getMembers();
		for (IService service : services) {
			MembershipService msm = (MembershipService) service;
			Member m = msm.getCoordinator(key);
			Assert.assertNotNull(m);
			List<Member> msmMembers = msm.getMembers();
			Assert.assertTrue(
					printInfo(p, mspMembers) + printInfo(m, msmMembers), 
					p.equals(m));
		}
		return p;
	}
	
	private String printInfo(Member me , List<Member> members) {
		String s = "Myself: " + me.toString();
		for (Member m : members) {
			s += ";; " + m.toString();
		}
		return s;
	}
}
