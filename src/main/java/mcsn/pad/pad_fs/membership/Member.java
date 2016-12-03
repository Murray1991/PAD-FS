package mcsn.pad.pad_fs.membership;
/**
 * 
 * @author Leonardo Gazzarri
 *
 */
public class Member {
	public final String host;
	public final int port;
	public final long heartbeat;
	public final String id;

	Member(String host, int port, long hearbeat, String id) {
		this.host = host;
		this.port = port;
		this.heartbeat = hearbeat;
		this.id = id;
	}
	
	@Override
	public String toString() {
		return "Member: " + "[host=" + host + ", port=" + port + ", heatbeat=" + heartbeat + ", id="+ id + "]";
	}
}
