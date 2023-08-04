package PageTable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class PageTable implements Serializable {

	public PageTable() throws IOException {
		File p = new File("src/main/resources/data/PageTable.ser");
		if (!p.exists()) {
			FileOutputStream fileOutputStream2 = new FileOutputStream("src/main/resources/data/PageTable.ser");
			Vector<String[]> a = new Vector<String[]>();
			ObjectOutputStream out1 = new ObjectOutputStream(
					new BufferedOutputStream(new FileOutputStream("src/main/resources/data/PageTable.ser")));
			try {
				out1.writeObject(a);
			} finally {
				out1.close();
			}
		}
		// a[0] = table name
		// a[1] page id
		// a[2] from
		// a[3] to
		// a[4] clustering key

	}

	public void add(String[] a, Hashtable<String, Object> h) throws IOException, ClassNotFoundException {
		FileOutputStream fileOutputStream = new FileOutputStream("src/main/resources/data/" + a[1] + ".ser");
		ObjectInputStream in = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
		Vector<String[]> k = new Vector<String[]>();
		k = (Vector<String[]>) in.readObject(); // read vector
		k.add(a);

		PrintWriter writer = new PrintWriter("src/main/resources/data/PageTable.ser");
		writer.print(""); // clear page
		writer.close();
		ObjectOutputStream out1 = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream("src/main/resources/data/PageTable.ser")));
		try {
			out1.writeObject(k);
		} finally {
			out1.close();
		}
		Vector<Hashtable<String, Object>> v = new Vector<Hashtable<String, Object>>();
		v.add(h);
		ObjectOutputStream out = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream("src/main/resources/data/" + a[1] + ".ser")));
		try {
			out.writeObject(v);
		} finally {
			out.close();
		}
		in.close();
	}

	public Vector<String[]> getPages() throws IOException, ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
		Vector<String[]> k = new Vector<String[]>();
		k = (Vector<String[]>) in.readObject(); // read vector
		in.close();
		return k;

	}

	public void updatePages(Vector<String[]> newP) throws FileNotFoundException, IOException, ClassNotFoundException {

		PrintWriter writer = new PrintWriter("src/main/resources/data/PageTable.ser");
		writer.print(""); // clear page
		writer.close();
		ObjectOutputStream out1 = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream("src/main/resources/data/PageTable.ser")));
		try {
			out1.writeObject(newP);
		} finally {
			out1.close();
		}

	}
}
