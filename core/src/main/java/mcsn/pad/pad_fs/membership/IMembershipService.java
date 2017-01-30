package mcsn.pad.pad_fs.membership;

import java.util.List;
import mcsn.pad.pad_fs.common.IService;

public interface IMembershipService extends IService {
	public Member getMyself();
	
	public List<Member> getMembers();
	
	public List<Member> getPreferenceList(String key, int N) throws InterruptedException;
	
	public Member getCoordinator(String key);

	public List<Member> getDeadMembers();
}
