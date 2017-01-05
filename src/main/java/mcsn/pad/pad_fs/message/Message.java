package mcsn.pad.pad_fs.message;

import java.io.Serializable;


public class Message implements Serializable {

	private static final long serialVersionUID = 319471187021291131L;
	
	public static final int MAX_PACKET_SIZE = 102400;
	
	// type codes
	public static final int PUT 	= 0;
	public static final int GET 	= 1;
	public static final int LIST 	= 2;
	public static final int REMOVE 	= 3;
	
	//type codes used only for replica management
	public static final int PUSH	= 4;
	public static final int PULL	= 5;
	public static final int REPLY	= 6;
	
	// status codes
	public static final int SUCCESS = 7;
	public static final int ERROR 	= 8;
	public static final int UNKNOWN = 9;
	public static final int OK 		= 7;
	public static final int KO 		= 8;

	public int type;
	
	public int status;
}
