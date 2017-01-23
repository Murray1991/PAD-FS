package mcsn.pad.pad_fs.membership;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;

public class MemberByteConverter implements ConsistentHasher.BytesConverter<Member> {

	@Override
	public byte[] convert(Member data) {
		return ConsistentHasher.getStringToBytesConverter().convert(data.id);
	}

}
