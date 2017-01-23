package mcsn.pad.pad_fs.message;

import voldemort.versioning.VectorClock;

public class InternalMessage extends Message {

	private static final long serialVersionUID = 776899032961015436L;

	public VectorClock vectorClock;
}
