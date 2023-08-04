package Octree;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Vector;

public class OctreeTable implements java.io.Serializable{
	public OctreeTable() throws Exception {
		File p = new File("src/main/resources/data/OctreeTable.ser");
		if (!p.exists()) {
			FileOutputStream fileOutputStream2 = new FileOutputStream("src/main/resources/data/OctreeTable.ser");
			Vector<String[]> a = new Vector<String[]>();
			ObjectOutputStream out1 = new ObjectOutputStream(
					new BufferedOutputStream(new FileOutputStream("src/main/resources/data/OctreeTable.ser")));
			try {
				out1.writeObject(a);
			} finally {
				out1.close();
			}
		}
		//a[0]=table name
		//a[1]=index name (tblName_col1_col2_col3)
		//a[1]=col1
		//a[2]=col2
		//a[3]=col3
	}
	public void add(String[] a) throws IOException, ClassNotFoundException {
		FileOutputStream fileOutputStream = new FileOutputStream("src/main/resources/data/" +a[0]+"."+ a[1] + ".ser");
		ObjectInputStream in = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/OctreeTable.ser")));
		Vector<String[]> k = new Vector<String[]>();
		k = (Vector<String[]>) in.readObject(); // read vector
		k.add(a);

		PrintWriter writer = new PrintWriter("src/main/resources/data/OctreeTable.ser");
		writer.print(""); // clear page
		writer.close();
		ObjectOutputStream out1 = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream("src/main/resources/data/OctreeTable.ser")));
		try {
			out1.writeObject(k);
		} finally {
			out1.close();
		}}
}
