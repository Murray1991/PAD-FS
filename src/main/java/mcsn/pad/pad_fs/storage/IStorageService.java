package mcsn.pad.pad_fs.storage;

import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.message.Message;

public interface IStorageService extends IService {
	public Message deliverMessage(Message msg);
}
