package mcsn.pad.pad_fs.common;

/**
 * 
 * @author Leonardo Gazzarri
 * 
 * Interface that defines a service that runs in a PAD-FS system
 *
 */
public interface IService {
	
	public void start();
	
	public void shutdown();
	
	public boolean isRunning();

}
