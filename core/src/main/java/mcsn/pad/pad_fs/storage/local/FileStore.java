package mcsn.pad.pad_fs.storage.local;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import voldemort.versioning.Versioned;

public class FileStore extends LocalStore {

	private Path path;
	
	private FileStore(String name, String prepend) {
		super(prepend+name);
	}
	
	public FileStore(String name) {
		super(name, new FileStore(name, "concurrent_"));
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Versioned<byte[]> get(Serializable key) {
		Versioned<byte[]> obj = null;
		try {
			Path p = Paths.get(path.toString(), key.toString());
			ObjectInputStream in = 
					new ObjectInputStream(
							new FileInputStream(p.toString()));
		    obj = (Versioned<byte[]>) in.readObject();
		    in.close();
		} catch (Exception e) {
			System.err.println("error in get");
		    e.printStackTrace();
		}
		return obj;
	}

	@Override
	public void put(Serializable key, Versioned<byte[]> value) {
		try {
			Path p = Paths.get(path.toString(), key.toString());
			ObjectOutputStream out = 
					new ObjectOutputStream(
							new FileOutputStream(p.toString()));
			out.writeObject(value);
			out.close();
		} catch (IOException e) {
			System.err.println("error in put");
			e.printStackTrace();
		}
	}

	@Override
	public Iterable<Serializable> list() {
		// TODO Auto-generated method stub
		return super.list();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

}
