package mcsn.pad.pad_fs.storage;

import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.message.ClientMessage;

public interface IStorageService extends IService {
	public ClientMessage deliverMessage(ClientMessage msg);
}
