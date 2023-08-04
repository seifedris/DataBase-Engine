package DBApp;

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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import Exceptions.DBAppException;
import Octree.Leaf;
import Octree.NLeaf;
import Octree.Octree;
import Octree.OctreeTable;
import Octree.Position;
import Octree.Range;
import Octree.Entry;
import PageTable.PageTable;
import SQLTerm.SQLTerm;

public class DBApp implements java.io.Serializable {
	private transient static PageTable pt;
	private transient static OctreeTable ot;

	public void init() throws Exception {
		pt = new PageTable();
		ot = new OctreeTable();
		FileWriter mtda = new FileWriter("src/main/resources/metadata.csv", true);
		mtda.close();
	}

	public void createTable(String tblName, String clustKey, Hashtable<String, String> columnType,
			Hashtable<String, String> columnMin, Hashtable<String, String> columnMax)
			throws DBAppException, IOException {

		Collection colName = columnType.keySet();
		Collection colType = columnType.values();
		Collection colMinName = columnMin.keySet();
		Collection colMaxName = columnMax.keySet();
		Collection colMin = columnMin.values();
		Collection colMax = columnMax.values();
		Iterator a = columnType.values().iterator();
		Iterator b = columnMin.keySet().iterator();
		Iterator c = columnMax.keySet().iterator();
		Iterator d = columnMin.values().iterator();
		Iterator e = columnMax.values().iterator();
		if (colMin.size() != colMax.size() || colName.size() != colMin.size() || colMax.size() != colName.size())
			throw new DBAppException("inconsistent column");
		boolean clust = false;
		for (String o : columnType.keySet()) {
			if (((String) o).equals(clustKey))
				clust = true;
		}
		if (!clust)
			throw new DBAppException("inconsistent column");

		FileWriter mtda = new FileWriter("src/main/resources/metadata.csv", true);
		for (String o : columnType.keySet()) {
			mtda.write(tblName + ",");
			mtda.write((String) o + ",");
			mtda.write((String) a.next() + ",");
			if (((String) o).equals(clustKey))
				mtda.write("True,");
			else
				mtda.write("False,");
			mtda.write("null,"); // index name
			mtda.write("null,"); // index type
			if (((String) b.next()).equals(((String) o)))
				mtda.write((Object) d.next() + ",");
			if (((String) c.next()).equals(((String) o)))
				mtda.write((Object) e.next() + "\n");
		}
		mtda.flush();
		mtda.close();

	}

	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException, Exception {
		ot = new OctreeTable();
		if (strarrColName[0] == null || strarrColName[1] == null || strarrColName[2] == null)
			throw new DBAppException();
		String[] clustRow = null;
		String indexName = sort(strarrColName[0], strarrColName[1], strarrColName[2]);
		ArrayList<String> cols = new ArrayList<String>();
		ArrayList<String> colTypes = new ArrayList<String>();
		ArrayList<String> colMin = new ArrayList<String>();
		ArrayList<String> colMax = new ArrayList<String>();
		ArrayList<String[]> allRows = new ArrayList<String[]>();
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] content = line.split(",");
			if (content[0].equals(strTableName) && (content[1].equals(strarrColName[0])
					|| content[1].equals(strarrColName[1]) || content[1].equals(strarrColName[2]))) {
				content[4] = indexName;
				content[5] = "Octree";
			}
			allRows.add(content);

			if (content[3].equals("True") && content[0].equals(strTableName))
				clustRow = content;
			if (content[0].equals(strTableName)) {

				cols.add(content[1]);
				colTypes.add(content[2]);
				colMin.add(content[6]);
				colMax.add(content[7]);
			}
			line = br.readLine();

		}
		br.close();

		FileWriter mtda = new FileWriter("src/main/resources/metadata.csv", false);
		mtda.write("");
		mtda = new FileWriter("src/main/resources/metadata.csv", true);
		for (String[] content : allRows) {
			mtda.write(content[0] + ",");
			mtda.write(content[1] + ",");
			mtda.write(content[2] + ",");
			mtda.write(content[3] + ",");
			mtda.write(content[4] + ",");
			mtda.write(content[5] + ",");
			mtda.write(content[6] + ",");
			mtda.write(content[7] + "\n");
		}
		mtda.flush();
		mtda.close();
		if (clustRow == null || cols.size() == 0)
			throw new DBAppException("There doesn't exist a table with this name.");

		String clustKeyType = clustRow[2];
		String clustKey = clustRow[1];

		Properties prop = new Properties();
		FileInputStream fis = new FileInputStream("src/main/resources/DBApp.config");
		prop.load(fis);
		fis.close();

		int entriesPerNode = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));

		FileOutputStream fileOutputStream = new FileOutputStream(
				"src/main/resources/data/" + strTableName + "." + indexName + ".ser");
		boolean second = false;
		boolean third = false;
		String col1 = "";
		String col2 = "";
		String col3 = "";
		for (int i = 0; i != indexName.length(); i++) {
			char c = indexName.charAt(i);
			if (c != '.' && !second && !third)
				col1 = col1 + c;
			else if (c == '.' && !second && !third)
				second = true;
			else if (c != '.' && second && !third)
				col2 = col2 + c;
			else if (c == '.' && second && !third)
				third = true;
			else if (c != '.' && second && third)
				col3 = col3 + c;

		}

		String[] octPage = { strTableName, indexName, col1, col2, col3 };
		ot.add(octPage);

		Object col1Min = null;
		Object col2Min = null;
		Object col3Min = null;
		Object col1Max = null;
		Object col2Max = null;
		Object col3Max = null;
		String col1Type = null;
		String col2Type = null;
		String col3Type = null;

		for (int i = 0; i != cols.size(); i++) {
			if (cols.get(i).equals(col1)) {
				col1Min = colMin.get(i);
				col1Max = colMax.get(i);
				col1Type = colTypes.get(i);
			}
			if (cols.get(i).equals(col2)) {
				col2Min = colMin.get(i);
				col2Max = colMax.get(i);
				col2Type = colTypes.get(i);

			}
			if (cols.get(i).equals(col3)) {
				col3Min = colMin.get(i);
				col3Max = colMax.get(i);
				col3Type = colTypes.get(i);
			}

		}
		col1Min = cast(col1Type, (String) col1Min);
		col2Min = cast(col2Type, (String) col2Min);
		col3Min = cast(col3Type, (String) col3Min);
		col1Max = cast(col1Type, (String) col1Max);
		col2Max = cast(col2Type, (String) col2Max);
		col3Max = cast(col3Type, (String) col3Max);
		new Octree(null, null, null, null, null, null, null, null, null).create(strTableName + "." + indexName,
				col1Type, col1Min, col1Max, col2Type, col2Min, col2Max, col3Type, col3Min, col3Max, entriesPerNode);

		ObjectInputStream in1 = new ObjectInputStream(new BufferedInputStream(
				new FileInputStream("src/main/resources/data/" + strTableName + "." + indexName + ".ser")));
		Octree index1 = (Octree) in1.readObject();
		in1.close();
		Vector<String[]> pages = new Vector();
		in1 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
		pages = (Vector<String[]>) in1.readObject();
		in1.close();
		int num = 0;
		for (String[] page : pages) {
			Vector<Hashtable<String, Object>> currPage = new Vector();
			if (page[0].equals(strTableName)) {
				in1 = new ObjectInputStream(
						new BufferedInputStream(new FileInputStream("src/main/resources/data/" + page[1] + ".ser")));
				currPage = (Vector<Hashtable<String, Object>>) in1.readObject();
				in1.close();
			}
			for (Hashtable row : currPage) {
				num++;
				Entry ent = new Entry(row.get(clustKey), page[1]);
				index1.insert(ent, col1, row.get(col1), col1Type, col2, row.get(col2), col2Type, col3, row.get(col3),
						col3Type, clustKey, clustKeyType);
			}
		}

		PrintWriter writer = new PrintWriter("src/main/resources/data/" + strTableName + "." + indexName + ".ser");
		writer.print(""); // clear page
		writer.close();
		ObjectOutputStream out1 = new ObjectOutputStream(new BufferedOutputStream(
				new FileOutputStream("src/main/resources/data/" + strTableName + "." + indexName + ".ser")));
		try {
			out1.writeObject(index1);
		} finally {
			out1.close();
		}

	}

	private Object cast(String type, String obj) throws ParseException {
		if (type.equals("java.lang.Integer"))
			return (int) Integer.parseInt(obj);
		if (type.equals("java.lang.Double"))
			return (double) Double.valueOf(obj);
		if (type.equals("java.util.Date")) {
			SimpleDateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy");
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
			if (obj.charAt(0) == '1' || obj.charAt(0) == '2')
				return (Date) formatter2.parse(obj);
			else
				return formatter.parse(obj);
		}
		return obj;

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> colNameValue) throws Exception { // check
																												// on
																												// maxes
																												// and
																												// mins
		String[] clustRow = null;
		int pg;
		int numOfRows = 0;
		ArrayList allCols = new ArrayList<String[]>();
		Properties prop = new Properties();
		FileInputStream fis = new FileInputStream("src/main/resources/DBApp.config");
		prop.load(fis);
		fis.close();

		numOfRows = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
		ArrayList<String> cols = new ArrayList<String>();
		ArrayList<String> colMin = new ArrayList<String>();
		ArrayList<String> colMax = new ArrayList<String>();
		ArrayList<String> colTypes = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] content = line.split(",");

			if (content[3].equals("True") && content[0].equals(strTableName))
				clustRow = content;
			if (content[0].equals(strTableName)) {
				allCols.add(content);
				cols.add(content[1]);
				colTypes.add(content[2]);
				colMin.add(content[6]);
				colMax.add(content[7]);
			}
			line = br.readLine();

		}
		br.close();
		if (clustRow == null || cols.size() == 0)
			throw new DBAppException("There doesn't exist a table with this name.");

		String clustKeyType = clustRow[2];
		String clustKey = clustRow[1];

		if (colNameValue.get(clustKey) == null)
			throw new DBAppException("Please enter a value for the Clustering Key");

		Vector<String[]> pages = new Vector();
		ObjectInputStream in1 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
		pages = (Vector<String[]>) in1.readObject();
		in1.close();
		boolean found = false;
		for (String[] s : pages) {
			if (s[0].equals(strTableName))
				found = true;
		}

		if (!found) {

			if ((pages.size() > 0)) {
				pg = Integer.parseInt((pages).lastElement()[1]) + 1;

			} else
				pg = 0;

			String[] s = { strTableName, pg + "", colNameValue.get(clustKey) + "", "" + colNameValue.get(clustKey),
					clustKey };
			for (int i = 0; i != cols.size(); i++) {
				if (colNameValue.get(cols.get(i)) == null) {
					colNameValue.put(cols.get(i), new Null());
					throw new DBAppException();

				}
			}

			pt.add(s, colNameValue);// create new page

			Entry ent = new Entry(colNameValue.get(clustKey), Integer.toString(pg));
			br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			line = br.readLine();
			String indexName = null;
			while (line != null) {
				String[] content = line.split(",");

				if (content[5].equals("Octree") && content[0].equals(strTableName))
					indexName = content[4];
				line = br.readLine();

			}
			br.close();
			boolean second = false;
			boolean third = false;
			String col1 = "";
			String col2 = "";
			String col3 = "";
			if (indexName != null) {
				for (int i = 0; i != indexName.length(); i++) { // get cols in order
					char c = indexName.charAt(i);
					if (c != '.' && !second && !third)
						col1 = col1 + c;
					else if (c == '.' && !second && !third)
						second = true;
					else if (c != '.' && second && !third)
						col2 = col2 + c;
					else if (c == '.' && second && !third)
						third = true;
					else if (c != '.' && second && third)
						col3 = col3 + c;

				}

				Object col1Value = colNameValue.get(col1);
				Object col2Value = colNameValue.get(col2);
				Object col3Value = colNameValue.get(col3);
				String col1Type = "";
				String col2Type = "";
				String col3Type = "";
				for (int i = 0; i != cols.size(); i++) { // get col types
					if (cols.get(i).equals(col1))
						col1Type = colTypes.get(i);
					if (cols.get(i).equals(col2))
						col2Type = colTypes.get(i);
					if (cols.get(i).equals(col3))
						col3Type = colTypes.get(i);
				}
				in1 = new ObjectInputStream(new BufferedInputStream(
						new FileInputStream("src/main/resources/data/" + strTableName + "." + indexName + ".ser")));
				Octree index1 = (Octree) in1.readObject();
				in1.close();
				index1.insert(ent, col1, col1Value, col1Type, col2, col2Value, col2Type, col3, col3Value, col3Type,
						clustKey, clustKeyType);
				PrintWriter writer = new PrintWriter(
						"src/main/resources/data/" + strTableName + "." + indexName + ".ser");
				writer.print(""); // clear page
				writer.close();
				ObjectOutputStream out1 = new ObjectOutputStream(new BufferedOutputStream(
						new FileOutputStream("src/main/resources/data/" + strTableName + "." + indexName + ".ser")));
				try {
					out1.writeObject(index1);
				} finally {
					out1.close();
				}

			}
		} else {

			Iterator it = colNameValue.keySet().iterator();
			while (it.hasNext()) {
				boolean exists = false;
				String column = (String) it.next();
				for (int i = 0; i != cols.size(); i++) {
					if (cols.get(i).equals(column))
						exists = true;
				}
				if (!exists)
					throw new DBAppException(column + " does not exist in this table");
			}

			for (int i = 0; i != cols.size(); i++) {
				if (colNameValue.get(cols.get(i)) == null) {
					colNameValue.put(cols.get(i), new Null());

				}

			}

			for (int i = 0; i != cols.size(); i++) {
				Object colmin = null;
				Object colmax = null;
				if (colTypes.get(i).equals("java.lang.Integer")) {
					colmin = Integer.parseInt(colMin.get(i));
					colmax = Integer.parseInt(colMax.get(i));
				}
				if (colTypes.get(i).equals("java.lang.String")) {
					colmin = colMin.get(i);
					colmax = colMax.get(i);
				}
				if (colTypes.get(i).equals("java.lang.Double")) {
					colmin = Double.valueOf(colMin.get(i));
					colmax = Double.valueOf(colMax.get(i));
				}
				if (colTypes.get(i).equals("java.util.Date")) {
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
					colmin = formatter.parse(colMin.get(i));
					colmax = formatter.parse(colMax.get(i));
				}

				if (compare(colNameValue.get(cols.get(i)), colmin, colTypes.get(i)) > 0
						&& !colNameValue.get(cols.get(i)).toString().equals("null")) {
					throw new DBAppException("minimum" + " " + cols.get(i));
				}
				if (compare(colNameValue.get(cols.get(i)), colmax, colTypes.get(i)) < 0
						&& colNameValue.get(cols.get(i)).toString().equals("null")) {
					throw new DBAppException("maximum");
				}

			}
			Vector<Hashtable<String, Object>> row = new Vector<Hashtable<String, Object>>();

			for (int i = 0; i != cols.size(); i++) {

				if (((colNameValue.get(cols.get(i)).getClass() == Integer.class
						&& colTypes.get(i).equals("java.lang.Integer"))
						|| (colNameValue.get(cols.get(i)).getClass() == String.class
								&& colTypes.get(i).equals("java.lang.String"))
						|| (colNameValue.get(cols.get(i)).getClass() == Date.class
								&& colTypes.get(i).equals("java.util.Date"))
						|| (colNameValue.get(cols.get(i)).getClass() == Double.class
								&& colTypes.get(i).equals("java.lang.Double")))) {

				}

				else if (!colNameValue.get(cols.get(i)).toString().equals("null"))
					throw new DBAppException(cols.get(i) + " should be of type " + colTypes.get(i));

			}
			addToPage(strTableName, colNameValue, numOfRows, clustKey, clustKeyType, allCols, cols, colTypes);
		}
	}

	private void addToPage(String strTableName, Hashtable<String, Object> colNameValue, int numOfRows, String clustKey,
			String clustKeyType, ArrayList<String[]> allCols, ArrayList<String> cols, ArrayList<String> colTypes)
			throws Exception {
		Vector<String[]> pages = new Vector<String[]>();

		Hashtable<String, Object> overflow = new Hashtable<String, Object>();
		ObjectInputStream in = null;
		ObjectInputStream in1 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
		pages = (Vector<String[]>) in1.readObject();

		Object value = null;
		if (clustKeyType.equals("java.lang.Integer"))
			value = (int) colNameValue.get(clustKey);

		if (clustKeyType.equals("java.lang.Double"))
			value = (double) colNameValue.get(clustKey);

		if (clustKeyType.equals("java.lang.String"))
			value = (String) colNameValue.get(clustKey);

		if (clustKeyType.equals("java.util.Date")) {
			value = (Date) colNameValue.get(clustKey);

		}

		Vector<Hashtable<String, Object>> p = new Vector<Hashtable<String, Object>>();
		String pageId = null;
		boolean a = false; // smallest value

		for (String[] line : pages) {

			if (line[0].equals(strTableName) && (compare(value, line[2], clustKeyType)) < 0) { // first row<value
				pageId = line[1];

				a = true;

			}
		}
		if (!a) { // smallest insertion yet
			for (String[] line : pages) {
				if (line[0].equals(strTableName)) {
					pageId = line[1];
					break;
				}
			}
		}

		in = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/" + pageId + ".ser")));
		p = (Vector<Hashtable<String, Object>>) in.readObject();
		Iterator it = p.iterator();
		int index = 0;
		while (it.hasNext()) {
			Hashtable<String, Object> next = (Hashtable<String, Object>) it.next();
			if (compare(value, next.get(clustKey), clustKeyType) < 0) // this row < clustKeyValue
				index++;
			if (compare(value, next.get(clustKey), clustKeyType) == 0)
				throw new DBAppException("primary key already exists");

		}
		p.add(index, colNameValue);
		if (p.size() > numOfRows) {
			overflow = p.lastElement();
			p.remove(p.size() - 1);
		}
		PrintWriter writer = new PrintWriter("src/main/resources/data/" + pageId + ".ser");
		writer.print(""); // clear page
		writer.close();

		ObjectOutputStream out1 = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream("src/main/resources/data/" + pageId + ".ser")));
		try {
			out1.writeObject(p);
		} finally {
			out1.close();
		}
		updatePage(pageId, clustKey);

		boolean ov = false;
		boolean done = false;
		String nextPage = null;
		ArrayList<Hashtable<String, Object>> overflows = new ArrayList<Hashtable<String, Object>>();
		ArrayList<String> ovpId = new ArrayList<String>();
		ObjectInputStream in11 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
		pages = (Vector<String[]>) in11.readObject();
		in11.close();
		if (!overflow.isEmpty()) {

			p = new Vector<Hashtable<String, Object>>();
			for (String[] line : pages) {
				if (ov && line[0].equals(strTableName) && !done) {
					nextPage = line[1];
					in = new ObjectInputStream(new BufferedInputStream(
							new FileInputStream("src/main/resources/data/" + nextPage + ".ser")));
					p = (Vector<Hashtable<String, Object>>) in.readObject();
					writer = new PrintWriter("src/main/resources/data/" + nextPage + ".ser");
					writer.print(""); // clear page
					writer.close();
					overflows.add(overflow);
					ovpId.add(nextPage);
					p.add(0, overflow);
					if (p.size() <= numOfRows) {
						done = true;

					}

					else {
						overflow = p.lastElement();
						p.remove(p.size() - 1);

					}

					out1 = new ObjectOutputStream(new BufferedOutputStream(
							new FileOutputStream("src/main/resources/data/" + nextPage + ".ser")));
					try {
						out1.writeObject(p);
					} finally {
						out1.close();
					}
					if (p.size() <= numOfRows)
						done = true;
					updatePage(nextPage, clustKey);
				}

				if (line[0].equals(strTableName) && pageId.equals(line[1])) { // found current page start searching
					ov = true;
				}

			}
			if (!done) {

				Vector<String[]> v = new Vector();
				in11 = new ObjectInputStream(
						new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
				v = (Vector<String[]>) in11.readObject();
				in1.close();
				int lastPage = Integer.parseInt(v.lastElement()[1]);
				String[] p1 = { strTableName, lastPage + 1 + "", "" + overflow.get(clustKey),
						"" + overflow.get(clustKey), clustKey };
				pt.add(p1, overflow);
				overflows.add(overflow);
				ovpId.add((lastPage + 1) + "");
				updatePage((lastPage + 1) + "", clustKey);
			}

		}

		in.close();
		writer.close();
		Vector<Object> primVals = new Vector();

		in1 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/" + pageId + ".ser")));
		Vector<Hashtable<String, Object>> page = (Vector<Hashtable<String, Object>>) in1.readObject();
		in1.close();

		boolean found = false;
		Iterator pIt = page.iterator();
		while (pIt.hasNext()) {
			Hashtable<String, Object> curr = (Hashtable<String, Object>) pIt.next();
			if (compare(curr.get(clustKey), colNameValue.get(clustKey), clustKeyType) == 0) {
				found = true;

				break;
			}
		}
		if (!found) {
			updatePage(nextPage, clustKey);
			in11 = new ObjectInputStream(
					new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
			pages = (Vector<String[]>) in11.readObject();
			in11.close();
			Iterator pagit = pages.iterator();
			while (pagit.hasNext()) {
				String[] pagEntry = (String[]) pagit.next();
				if (pagEntry[0].equals(strTableName)
						&& compare(colNameValue.get(clustKey), pagEntry[2], clustKeyType) <= 0
						&& compare(colNameValue.get(clustKey), pagEntry[3], clustKeyType) >= 0) {
					pageId = pagEntry[1];
					found = true;
				}

			}
		}

		ovpId.add(pageId);
		overflows.add(colNameValue);
		for (Hashtable h : overflows) {
			primVals.add(h.get(clustKey));
		}
		ArrayList<String> indices = new ArrayList();

		for (String[] content : allCols) {
			if (content[5].equals("Octree") && indices.indexOf(content[4]) == -1) {

				in1 = new ObjectInputStream(new BufferedInputStream(
						new FileInputStream("src/main/resources/data/" + strTableName + "." + content[4] + ".ser")));
				Octree index1 = (Octree) in1.readObject();
				in1.close();

				indices.add(content[4]);
				boolean second = false;
				boolean third = false;
				String col1 = "";
				String col2 = "";
				String col3 = "";
				for (int i = 0; i != content[4].length(); i++) { // get cols in order
					char c = content[4].charAt(i);
					if (c != '.' && !second && !third)
						col1 = col1 + c;
					else if (c == '.' && !second && !third)
						second = true;
					else if (c != '.' && second && !third)
						col2 = col2 + c;
					else if (c == '.' && second && !third)
						third = true;
					else if (c != '.' && second && third)
						col3 = col3 + c;

				}
				second = false;
				third = false;
				String col1Type = "";
				String col2Type = "";
				String col3Type = "";
				for (int i = 0; i != cols.size(); i++) { // get col types
					if (cols.get(i).equals(col1))
						col1Type = colTypes.get(i);
					if (cols.get(i).equals(col2))
						col2Type = colTypes.get(i);
					if (cols.get(i).equals(col3))
						col3Type = colTypes.get(i);
				}
				for (int j = 0; j != overflows.size(); j++) {

					Hashtable row = overflows.get(j);
					Entry entry = new Entry(row.get(clustKey), (String) ovpId.get(j));
					Object col1Value = row.get(col1);
					Object col2Value = row.get(col2);
					Object col3Value = row.get(col3);

					index1.updatePageId(entry, col1, col1Value, col1Type, col2, col2Value, col2Type, col3, col3Value,
							col3Type, clustKey, clustKeyType);
				}
				index1.insert(new Entry(colNameValue.get(clustKey), pageId), col1, colNameValue.get(col1), col1Type,
						col2, colNameValue.get(col2), col2Type, col3, colNameValue.get(col3), col3Type, clustKey,
						clustKeyType);

				writer = new PrintWriter("src/main/resources/data/" + strTableName + "." + content[4] + ".ser");
				writer.print(""); // clear page
				writer.close();
				out1 = new ObjectOutputStream(new BufferedOutputStream(
						new FileOutputStream("src/main/resources/data/" + strTableName + "." + content[4] + ".ser")));
				try {
					out1.writeObject(index1);
				} finally {
					out1.close();
				}

			}
		}

	}

	private void updatePage(String pageName, String clustKey) throws ClassNotFoundException, IOException {
		ObjectInputStream in = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/" + pageName + ".ser")));
		Vector<Hashtable<String, Object>> a = (Vector<Hashtable<String, Object>>) in.readObject();
		String beg = "" + a.firstElement().get(clustKey);
		String end = "" + a.lastElement().get(clustKey);
		Vector<String[]> ss = new Vector();
		ObjectInputStream in1 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
		ss = (Vector<String[]>) in1.readObject();
		in1.close();
		for (String[] page : ss) {
			if (page[1].equals(pageName)) {
				page[2] = beg;
				page[3] = end;
				break;
			}
		}

		pt.updatePages(ss);
		in.close();
		in1.close();

	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws Exception {
		String[] clustRow = null;
		ArrayList<String[]> allCols = new ArrayList<String[]>();
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line1 = br.readLine();
		ArrayList<String> colTypes = new ArrayList<String>();
		ArrayList<String> cols = new ArrayList<String>();
		BufferedReader br1 = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = br1.readLine();
		while (line != null) {
			String[] content = line.split(",");

			if (content[3].equals("True") && content[0].equals(strTableName))
				clustRow = content;
			if (content[0].equals(strTableName)) {
				cols.add(content[1]);
				colTypes.add(content[2]);
				allCols.add(content);
			}
			line = br1.readLine();

		}
		br1.close();
		boolean exists;
		Iterator it1 = htblColNameValue.keySet().iterator();
		while (it1.hasNext()) {
			exists = false;
			String column = (String) it1.next();
			for (int i = 0; i != cols.size(); i++) {
				if (cols.get(i).equals(column))
					exists = true;
			}
			if (!exists)
				throw new DBAppException(column + " does not exist in this table");
		}

		if (clustRow == null) // page not found
			throw new DBAppException("There doesn't exist a table with this name.");

		String clustKeyType = clustRow[2];
		String clustKey = clustRow[1];
		String indexName = clustRow[4];

		Vector<Hashtable<String, Object>> p = new Vector<Hashtable<String, Object>>();
		String pageId = null;
		Vector<String[]> pages = new Vector<String[]>();
		ObjectInputStream in1 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
		pages = (Vector<String[]>) in1.readObject();
		in1.close();
		for (String[] line11 : pages) {
			if (strClusteringKeyValue != null)
				if (line11[0].equals(strTableName) && ((compare(strClusteringKeyValue, line11[2], clustKeyType) <= 0)
						&& ((compare(strClusteringKeyValue, line11[3], clustKeyType) >= 0)))) {
					pageId = line11[1];
					break;
				}

		}
		if (pageId == null) {

			throw new DBAppException("page not found");
		}
		if (!indexName.equals("null")) {
			in1 = new ObjectInputStream(new BufferedInputStream(
					new FileInputStream("src/main/resources/data/" + strTableName + "." + indexName + ".ser")));

			Octree index = (Octree) in1.readObject();
			in1.close();
			boolean second = false;
			boolean third = false;
			String col1 = "";
			String col2 = "";
			String col3 = "";
			for (int i = 0; i != indexName.length(); i++) { // get cols in order
				char c = indexName.charAt(i);
				if (c != '.' && !second && !third)
					col1 = col1 + c;
				else if (c == '.' && !second && !third)
					second = true;
				else if (c != '.' && second && !third)
					col2 = col2 + c;
				else if (c == '.' && second && !third)
					third = true;
				else if (c != '.' && second && third)
					col3 = col3 + c;

			}
			int colNum = 0;
			if (clustKey.equals(col1))
				colNum = 1;
			if (clustKey.equals(col2))
				colNum = 2;
			if (clustKey.equals(col3))
				colNum = 3;

			ObjectInputStream in = new ObjectInputStream(
					new BufferedInputStream(new FileInputStream("src/main/resources/data/" + pageId + ".ser")));
			p = (Vector<Hashtable<String, Object>>) in.readObject();
			in.close();
			Vector<Hashtable<String, Object>> fromIndex = index.selectEqual(clustKey, clustKeyType,
					cast(clustKeyType, strClusteringKeyValue), colNum, clustKey, clustKeyType, new Vector());

			if (fromIndex.size() == 0)
				return;

			Hashtable<String, Object> row = fromIndex.get(0);
			int k = p.indexOf(row);
			p.remove(row);
			Iterator itCol = htblColNameValue.keySet().iterator();
			ArrayList<String> col = new ArrayList<String>(); // add here
			while (itCol.hasNext())
				col.add((String) itCol.next());

			for (int i = 0; i != col.size(); i++) {
				if (row != null && col.get(i) != null && htblColNameValue.get(col.get(i)) != null
						&& row.get(col.get(i)) != null)
					row.replace(col.get(i), row.get(col.get(i)), htblColNameValue.get(col.get(i)));

			}
			p.add(k, row);

			Iterator it = p.iterator();
			in.close();
			Object col1Value = row.get(col1);
			Object col2Value = row.get(col2);
			Object col3Value = row.get(col3);
			String col1Type = "";
			String col2Type = "";
			String col3Type = "";
			for (int i = 0; i != cols.size(); i++) {
				// get col types
				if (cols.get(i).equals(col1))
					col1Type = colTypes.get(i);
				if (cols.get(i).equals(col2))
					col2Type = colTypes.get(i);
				if (cols.get(i).equals(col3))
					col3Type = colTypes.get(i);
			}
			Entry entry = new Entry(cast(clustKeyType, strClusteringKeyValue), pageId);
			index.update(cast(clustKeyType, strClusteringKeyValue), entry, col1, col1Value, col1Type, col2, col2Value,
					col2Type, col3, col3Value, col3Type, clustKey, clustKeyType);
			PrintWriter writer = new PrintWriter("src/main/resources/data/" + strTableName + "." + indexName + ".ser");
			writer.print(""); // clear page
			writer.close();
			ObjectOutputStream out1 = new ObjectOutputStream(new BufferedOutputStream(
					new FileOutputStream("src/main/resources/data/" + strTableName + "." + indexName + ".ser")));
			try {
				out1.writeObject(index);
			} finally {
				out1.close();
			}
			PrintWriter writer1 = new PrintWriter("src/main/resources/data/" + pageId + ".ser");
			writer1.print(""); // clear page
			writer1.close();

			ObjectOutputStream out11 = new ObjectOutputStream(
					new BufferedOutputStream(new FileOutputStream("src/main/resources/data/" + pageId + ".ser")));
			try {
				out11.writeObject(p);
			} finally {
				out11.close();
			}
			updatePage(pageId, clustKey);
			in1.close();
			writer1.close();
			return;

		}

		ObjectInputStream in = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/" + pageId + ".ser")));
		p = (Vector<Hashtable<String, Object>>) in.readObject();
		Iterator it = p.iterator();
		int index = 0;
		while (it.hasNext()) {
			Hashtable<String, Object> next = (Hashtable<String, Object>) it.next();

			if (compare(strClusteringKeyValue, next.get(clustKey), clustKeyType) < 0) { // this row < clustKeyValue
				index++;
			}

		}
		Iterator itCol = htblColNameValue.keySet().iterator(); // get columns from input hashtable
		ArrayList<String> col = new ArrayList<String>(); // add here
		while (itCol.hasNext())
			col.add((String) itCol.next());

		for (int i = 0; i != col.size(); i++) {
			if (p.get(index) != null && col.get(i) != null && htblColNameValue.get(col.get(i)) != null
					&& p.get(index).get(col.get(i)) != null)
				p.get(index).replace(col.get(i), p.get(index).get(col.get(i)), htblColNameValue.get(col.get(i))); // replace
																													// old
																													// value
		}
		Entry entry = new Entry(cast(clustKeyType, strClusteringKeyValue), pageId);
		ArrayList<String> indices = new ArrayList();
		for (String[] content : allCols) {
			if (content[5].equals("Octree") && indices.indexOf(content[4]) == -1) {
				indices.add(content[4]);
				boolean second = false;
				boolean third = false;
				String col1 = "";
				String col2 = "";
				String col3 = "";
				for (int i = 0; i != content[4].length(); i++) { // get cols in order
					char c = content[4].charAt(i);
					if (c != '.' && !second && !third)
						col1 = col1 + c;
					else if (c == '.' && !second && !third)
						second = true;
					else if (c != '.' && second && !third)
						col2 = col2 + c;
					else if (c == '.' && second && !third)
						third = true;
					else if (c != '.' && second && third)
						col3 = col3 + c;

				}
				second = false;
				third = false;

				Object col1Value = p.get(index).get(col1);
				Object col2Value = p.get(index).get(col2);
				Object col3Value = p.get(index).get(col3);
				String col1Type = "";
				String col2Type = "";
				String col3Type = "";
				for (int i = 0; i != cols.size(); i++) { // get col types
					if (cols.get(i).equals(col1))
						col1Type = colTypes.get(i);
					if (cols.get(i).equals(col2))
						col2Type = colTypes.get(i);
					if (cols.get(i).equals(col3))
						col3Type = colTypes.get(i);
				}
				in1 = new ObjectInputStream(new BufferedInputStream(
						new FileInputStream("src/main/resources/data/" + strTableName + "." + content[4] + ".ser")));
				Octree index1 = (Octree) in1.readObject();
				in1.close();
				index1.update(cast(clustKeyType, strClusteringKeyValue), entry, col1, col1Value, col1Type, col2,
						col2Value, col2Type, col3, col3Value, col3Type, clustKey, clustKeyType);
				PrintWriter writer = new PrintWriter(
						"src/main/resources/data/" + strTableName + "." + content[4] + ".ser");
				writer.print(""); // clear page
				writer.close();
				ObjectOutputStream out1 = new ObjectOutputStream(new BufferedOutputStream(
						new FileOutputStream("src/main/resources/data/" + strTableName + "." + content[4] + ".ser")));
				try {
					out1.writeObject(index1);
				} finally {
					out1.close();
				}

			}

		}

		PrintWriter writer = new PrintWriter("src/main/resources/data/" + pageId + ".ser");
		writer.print(""); // clear page
		writer.close();

		ObjectOutputStream out1 = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream("src/main/resources/data/" + pageId + ".ser")));
		try {
			out1.writeObject(p);
		} finally {
			out1.close();
		}
		updatePage(pageId, clustKey);
		in.close();
		writer.close();

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws Exception {
		Vector<Object> primVals = new Vector<Object>();
		String clustKey = getClustKey(strTableName);
		ArrayList<String[]> allCols = new ArrayList<String[]>();
		String clustKeyType = getClustKeyType(strTableName);
		Collection key = htblColNameValue.keySet();
		Collection<Object> value = htblColNameValue.values();
		Iterator keysit = key.iterator();
		Iterator valuesit = value.iterator();
		ArrayList<String> keys = new ArrayList<String>();
		ArrayList<Object> values = new ArrayList<Object>();

		boolean deleteAny = false; // any records were deleted
		while (keysit.hasNext()) {
			keys.add((String) keysit.next());
			values.add(valuesit.next());
		}

		ArrayList<String> cols = new ArrayList<String>();
		ArrayList<String> colTypes = new ArrayList<String>();
		ArrayList<String> colIndex = new ArrayList<String>();
		BufferedReader br1 = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = br1.readLine();
		boolean octreeFound = false;
		while (line != null) {
			String[] content = line.split(",");
			if (content[0].equals(strTableName)) {
				cols.add(content[1]);
				colTypes.add(content[2]);
				allCols.add(content);
				colIndex.add(content[4]);
				if (!content[4].equals("null"))
					octreeFound = true;

			}

			line = br1.readLine();

		}
		br1.close();
		

		Iterator it1 = htblColNameValue.keySet().iterator();
		while (it1.hasNext()) {
			boolean exists = false;
			String column = (String) it1.next();
			for (int i = 0; i != cols.size(); i++) {
				if (cols.get(i).equals(column))
					exists = true;
			}
			if (!exists) {

				throw new DBAppException(column + " does not exist in this table");
			}
		}
		String pageId = null;
		boolean clust = false;
		if (octreeFound) {
			valsFromIndex(strTableName, htblColNameValue, cols, colTypes, colIndex, clustKey, clustKeyType);
			return;
		}

		if (htblColNameValue.get(clustKey) != null) {
			Vector<String[]> pages = new Vector<String[]>();
			ObjectInputStream in1 = new ObjectInputStream(
					new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
			pages = (Vector<String[]>) in1.readObject();
			in1.close();
			Iterator it = pages.iterator();
			String pagid = null;
			int pindex = 0;
			while (it.hasNext()) {
				String[] cont = (String[]) it.next();
				if (cont[0].equals(strTableName) && compare(htblColNameValue.get(clustKey), cont[2], clustKeyType) <= 0
						&& compare(htblColNameValue.get(clustKey), cont[3], clustKeyType) >= 0) {
					pagid = cont[1];
					break;
				} else
					pindex++;
			}
			Vector<Hashtable<String, Object>> p = new Vector<Hashtable<String, Object>>();
			ObjectInputStream in = new ObjectInputStream(
					new BufferedInputStream(new FileInputStream("src/main/resources/data/" + pagid + ".ser")));
			p = (Vector<Hashtable<String, Object>>) in.readObject();
			int index = 0;

			Iterator pageit = p.iterator();
			PrintWriter writer = new PrintWriter("src/main/resources/data/" + pagid + ".ser");
			writer.print(""); // clear page
			writer.close();
			while (pageit.hasNext()) {
				Hashtable<String, Object> row = (Hashtable<String, Object>) pageit.next();
				if (compare(row.get(clustKey), htblColNameValue.get(clustKey), clustKeyType) != 0)
					index++;
				else
					break;
			}
			primVals.add(htblColNameValue.get(clustKey));
			p.remove(index);
			if (p.size() == 0) {
				File file = new File("src/main/resources/data/" + pagid + ".ser");
				file.delete();
				pages.remove(pindex);
				writer = new PrintWriter("src/main/resources/data/PageTable.ser");
				writer.print(""); // clear page table page
				writer.close();

				ObjectOutputStream out1 = new ObjectOutputStream(
						new BufferedOutputStream(new FileOutputStream("src/main/resources/data/PageTable.ser")));
				try {
					out1.writeObject(pages);
				} finally {
					out1.close();
				}

			} else {
				ObjectOutputStream out1 = new ObjectOutputStream(
						new BufferedOutputStream(new FileOutputStream("src/main/resources/data/" + pagid + ".ser")));
				try {
					out1.writeObject(p);
				} finally {
					out1.close();
				}
				updatePage(pagid, clustKey);
				in.close();
				writer.close();

			}
		}

		if (!clust) {

			Vector<Hashtable<String, Object>> p = new Vector<Hashtable<String, Object>>();

			Vector<String[]> pages = new Vector<String[]>();
			ObjectInputStream in1 = new ObjectInputStream(
					new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
			pages = (Vector<String[]>) in1.readObject();
			in1.close();
			Vector<String[]> pages2 = new Vector();
			in1 = new ObjectInputStream(
					new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
			pages2 = (Vector<String[]>) in1.readObject();
			in1.close();
			Iterator ptit = pages.iterator();
			int pindex = 0;
			for (int l = 0; l != pages.size(); l++) {
				String[] s = null;
				if (ptit.hasNext())
					s = (String[]) ptit.next();
				else
					break;
				if (s[0].equals(strTableName)) {
					pageId = s[1];
					ObjectInputStream in = new ObjectInputStream(
							new BufferedInputStream(new FileInputStream("src/main/resources/data/" + pageId + ".ser")));
					p = (Vector<Hashtable<String, Object>>) in.readObject();
					int index = 0;
					Iterator pageit = p.iterator();
					PrintWriter writer = new PrintWriter("src/main/resources/data/" + pageId + ".ser");
					writer.print(""); // clear page
					writer.close();

					while (pageit.hasNext()) { // loop over entries
						int i = 0;
						boolean del = true;
						Hashtable<String, Object> row = (Hashtable<String, Object>) pageit.next(); // to get every
																									// column
																									// without .next()
						for (String str : keys) {// loop over set to check values

							if (row.get(str) != null)
								if (!(row).get(str).equals(values.get(i))) { // check for
																				// values
									del = false;
								}
							i++;
						}
						if (del) { // all values are the same
							deleteAny = true;
							primVals.add(p.get(index).get(clustKey));
							p.remove(index);
							pageit = p.iterator();
							index = 0; // iterator restarts
							if (p.size() == 0) { // delete file

								File file = new File("src/main/resources/data/" + pageId + ".ser");
								file.delete();
								pages2.remove(pindex);
								l++; // for loop
								writer = new PrintWriter("src/main/resources/data/PageTable.ser");
								writer.print(""); // clear page table page
								writer.close();
								ptit.remove(); // remove page from iterator

								ObjectOutputStream out1 = new ObjectOutputStream(new BufferedOutputStream(
										new FileOutputStream("src/main/resources/data/PageTable.ser")));
								try {
									out1.writeObject(pages2);
								} finally {
									out1.close();
								}

								pindex--;
								break;
							} else {
								ObjectOutputStream out1 = new ObjectOutputStream(new BufferedOutputStream(
										new FileOutputStream("src/main/resources/data/" + pageId + ".ser")));
								try {
									out1.writeObject(p);
								} finally {
									out1.close();
								}
								updatePage(pageId, clustKey);
								in.close();
								writer.close();

							}

						} else {
							ObjectOutputStream out1 = new ObjectOutputStream(new BufferedOutputStream(
									new FileOutputStream("src/main/resources/data/" + pageId + ".ser")));
							try {
								out1.writeObject(p);
							} finally {
								out1.close();
							}
							updatePage(pageId, clustKey);
							in.close();
							writer.close();
							index++;
						}

					}

				}
				pindex++;
			}
		}

		ArrayList<String> indices = new ArrayList();
		for (String[] content : allCols) {
			if (content[5].equals("Octree") && indices.indexOf(content[4]) == -1) {
				indices.add(content[4]);
				boolean second = false;
				boolean third = false;
				String col1 = "";
				String col2 = "";
				String col3 = "";
				for (int i = 0; i != content[4].length(); i++) { // get cols in order
					char c = content[4].charAt(i);
					if (c != '.' && !second && !third)
						col1 = col1 + c;
					else if (c == '.' && !second && !third)
						second = true;
					else if (c != '.' && second && !third)
						col2 = col2 + c;
					else if (c == '.' && second && !third)
						third = true;
					else if (c != '.' && second && third)
						col3 = col3 + c;

				}
				second = false;
				third = false;

				ObjectInputStream in1 = new ObjectInputStream(new BufferedInputStream(
						new FileInputStream("src/main/resources/data/" + strTableName + "." + content[4] + ".ser")));
				Octree index1 = (Octree) in1.readObject();
				in1.close();
				index1.delete(primVals, clustKeyType);
				PrintWriter writer = new PrintWriter(
						"src/main/resources/data/" + strTableName + "." + content[4] + ".ser");
				writer.print(""); // clear page
				writer.close();
				ObjectOutputStream out1 = new ObjectOutputStream(new BufferedOutputStream(
						new FileOutputStream("src/main/resources/data/" + strTableName + "." + content[4] + ".ser")));
				try {
					out1.writeObject(index1);
				} finally {
					out1.close();
				}

			}

		}

	}

	private void valsFromIndex(String strTableName, Hashtable htblColNameValue, ArrayList<String> cols,
			ArrayList<String> colTypes, ArrayList<String> colIndex, String clustKey, String clustKeyType)
			throws Exception {
		ArrayList<Vector> total = new ArrayList();
		String colName = "";
		for (int i = 0; i != cols.size(); i++) {
			Vector<Hashtable> vec = new Vector();
			if (!colIndex.get(i).equals("null") && htblColNameValue.get(cols.get(i)) != null) {
				colName = colIndex.get(i);
				
				boolean second = false;
				boolean third = false;
				String col1 = "";
				String col2 = "";
				String col3 = "";
				for (int j = 0; j != colIndex.get(i).length(); j++) { // get cols in order
					char c = colIndex.get(i).charAt(j);
					if (c != '.' && !second && !third)
						col1 = col1 + c;
					else if (c == '.' && !second && !third)
						second = true;
					else if (c != '.' && second && !third)
						col2 = col2 + c;
					else if (c == '.' && second && !third)
						third = true;
					else if (c != '.' && second && third)
						col3 = col3 + c;

				}
				second = false;
				third = false;
				int colNum = 0;
				if (cols.get(i).equals(col1))
					colNum = 1;
				if (cols.get(i).equals(col2))
					colNum = 2;
				if (cols.get(i).equals(col3))
					colNum = 3;

				Object colValue = htblColNameValue.get(cols.get(i));

				String colType = colTypes.get(i);

				ObjectInputStream in1 = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
						"src/main/resources/data/" + strTableName + "." + colIndex.get(i) + ".ser")));
				Octree index1 = (Octree) in1.readObject();
				in1.close();
				vec = index1.selectEqual(cols.get(i), colType, colValue, colNum, clustKey, clustKeyType, new Vector());
				
				total.add(vec);
				htblColNameValue.remove(cols.get(i));
			}
			
		}
		while (total.size() > 1) {
			Vector<Hashtable> a = total.get(0);
			Vector<Hashtable> b = total.get(1);
			int s=a.size();
			for (int i=0;i!=s;i++) {
				Hashtable curr=a.get(i);
				if (!b.contains(curr)) {
					a.remove(curr);
					s=a.size();
					i--;
				}
			}
			total.remove(1);
		}
		
		Vector<Hashtable> finalVec = total.get(0);
		Vector<Object> primVals = new Vector();
		for (int i = 0; i != cols.size(); i++) {
			String col = cols.get(i);
			if (htblColNameValue.get(col) != null) {
				int k=finalVec.size();
				for (int i1=0;i1!=k;i1++) {
					Hashtable curr = finalVec.get(i1);
					if (compare(htblColNameValue.get(col), curr.get(col), colTypes.get(i)) != 0)
					{
						finalVec.remove(curr);
						i1--;
						k=finalVec.size();
						
				}}
			}
		}
		for(Hashtable curr:finalVec) {
			primVals.add(curr.get(clustKey));
		}
	
		ObjectInputStream in1 = new ObjectInputStream(new BufferedInputStream(
				new FileInputStream("src/main/resources/data/" + strTableName + "." + colName + ".ser")));
		Octree index1 = (Octree) in1.readObject();
		Vector<Entry> pageIds = index1.delete(primVals, clustKeyType);
		PrintWriter writer = new PrintWriter("src/main/resources/data/" + strTableName + "." + colName + ".ser");
		writer.print(""); // clear page table page
		writer.close();

		ObjectOutputStream out1 = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + "." + colName + ".ser")));
		try {
			out1.writeObject(index1);
		} finally {
			out1.close();
		}
		
		in1.close();
		if(pageIds==null)
			return;
		for (int i = 0; i != pageIds.size(); i++) {
			Hashtable row=getRow(pageIds.get(i).getValue(),pageIds.get(i).getPageId(),clustKey,clustKeyType);
			in1 = new ObjectInputStream(
					new BufferedInputStream(new FileInputStream("src/main/resources/data/" + pageIds.get(i).getPageId() + ".ser")));
			Vector page = (Vector) in1.readObject();
			page.remove(row);
			if (page.size() == 0) { // delete file

				File file = new File("src/main/resources/data/" + pageIds.get(i) + ".ser");
				file.delete();
				in1 = new ObjectInputStream(
						new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
				Vector pages = (Vector) in1.readObject();
				pages.remove(pageIds.get(i).getPageId());

				 writer = new PrintWriter("src/main/resources/data/PageTable.ser");
				writer.print(""); // clear page table page
				writer.close();

				 out1 = new ObjectOutputStream(
						new BufferedOutputStream(new FileOutputStream("src/main/resources/data/PageTable.ser")));
				try {
					out1.writeObject(pages);
				} finally {
					out1.close();
				}

			} else {
				 out1 = new ObjectOutputStream(new BufferedOutputStream(
						new FileOutputStream("src/main/resources/data/" + pageIds.get(i).getPageId() + ".ser")));
				try {
					out1.writeObject(page);
				} finally {
					out1.close();
				}
				updatePage(pageIds.get(i).getPageId(), clustKey);
				

			}
			

		}
		

	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException, Exception {
		String strTableName = arrSQLTerms[0]._strTableName;
		String col1Name = arrSQLTerms[0]._strColumnName;
		BufferedReader br1 = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = br1.readLine();
		String indexName = null;
		ArrayList<Vector> vectors = new ArrayList<Vector>();
		ArrayList<String> cols = new ArrayList<String>();
		String[] clustRow = null;
		ArrayList<String> colTypes = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line1 = br.readLine();

		while (line1 != null) {
			String[] content = line1.split(",");
			if (content[3].equals("True") && content[0].equals(strTableName))
				clustRow = content;
			if (content[0].equals(strTableName)) {
				cols.add(content[1]);
				colTypes.add(content[2]);
			}
			line1 = br.readLine();

		}
		br.close();
		if (clustRow == null || cols.size() == 0)
			throw new DBAppException("There doesn't exist a table with this name.");

		String clustKeyType = clustRow[2];
		String clustKey = clustRow[1];

		while (line != null) {
			String[] content = line.split(",");
			if (content[0].equals(strTableName) && content[5].equals("Octree")) {
				indexName = content[4];
			}

			line = br1.readLine();

		}
		br1.close();
		String col1 = "";
		String col2 = "";
		String col3 = "";
		boolean second = false;
		boolean third = false;
		if (indexName != null)
			for (int i = 0; i != indexName.length(); i++) { // get cols in order
				char c = indexName.charAt(i);
				if (c != '.' && !second && !third)
					col1 = col1 + c;
				else if (c == '.' && !second && !third)
					second = true;
				else if (c != '.' && second && !third)
					col2 = col2 + c;
				else if (c == '.' && second && !third)
					third = true;
				else if (c != '.' && second && third)
					col3 = col3 + c;

			}
		boolean useOctree = false;
		String sortedCols;
		if (arrSQLTerms.length == 3) {
			sortedCols = sort(arrSQLTerms[0]._strColumnName, arrSQLTerms[1]._strColumnName,
					arrSQLTerms[2]._strColumnName);
			if (sortedCols.equals(indexName))
				useOctree = true;

			for (String op : strarrOperators) {
				if (!op.equals("AND"))
					useOctree = false;
			}
		}
		if (useOctree) {

			ObjectInputStream in1 = new ObjectInputStream(new BufferedInputStream(
					new FileInputStream("src/main/resources/data/" + strTableName + "." + indexName + ".ser")));
			Octree index = (Octree) in1.readObject();
			in1.close();

			for (int i = 0; i != arrSQLTerms.length; i++) {

				int colNum = 0;
				SQLTerm term = arrSQLTerms[i];
				if (term._strColumnName.equals(col1))
					colNum = 1;
				if (term._strColumnName.equals(col2))
					colNum = 2;
				if (term._strColumnName.equals(col3))
					colNum = 3;
				Vector<Hashtable<String, Object>> relation = new Vector();
				int k = 0;
				for (String col : cols) {
					if (col.equals(term._strColumnName))
						break;
					k++;
				}
				if (term._strOperator.equals("="))
					relation = index.selectEqual(term._strColumnName, colTypes.get(k), term._objValue, colNum, clustKey,
							clustKeyType, new Vector());
				if (term._strOperator.equals("!=")) {
					relation = index.selectAll(clustKey, clustKeyType, new Vector());
					Vector<Hashtable<String, Object>> relation2 = index.selectEqual(term._strColumnName,
							colTypes.get(k), term._objValue, colNum, clustKey, clustKeyType, new Vector());
					relation.removeAll(relation2);

				}
				if (term._strOperator.equals(">="))
					relation = index.selectGreaterThanEqual(term._strColumnName, colTypes.get(k), term._objValue,
							colNum, clustKey, clustKeyType, new Vector());
				if (term._strOperator.equals("<="))
					relation = index.selectLessThanEqual(term._strColumnName, colTypes.get(k), term._objValue, colNum,
							clustKey, clustKeyType, new Vector());
				if (term._strOperator.equals(">"))
					relation = index.selectGreaterThan(term._strColumnName, colTypes.get(k), term._objValue, colNum,
							clustKey, clustKeyType, new Vector());
				if (term._strOperator.equals("<"))
					relation = index.selectLessThan(term._strColumnName, colTypes.get(k), term._objValue, colNum,
							clustKey, clustKeyType, new Vector());
				vectors.add(relation);
			}

		} else {

			Vector<String[]> pages = new Vector<String[]>();
			ObjectInputStream in1 = new ObjectInputStream(
					new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
			pages = (Vector<String[]>) in1.readObject();
			in1.close();

			for (SQLTerm term : arrSQLTerms) {

				Vector vec = new Vector();
				String colType = null;
				for (int i = 0; i != cols.size(); i++) {
					if (cols.get(i).equals(term._strColumnName))
						colType = colTypes.get(i);
				}

				for (String[] page : pages) {
					if (page[0].equals(strTableName)) {
						Vector<Hashtable<String, Object>> currPage = new Vector();
						in1 = new ObjectInputStream(new BufferedInputStream(
								new FileInputStream("src/main/resources/data/" + page[1] + ".ser")));
						currPage = (Vector<Hashtable<String, Object>>) in1.readObject();
						in1.close();
						for (Hashtable row : currPage) {
							if (term._strOperator.equals("=")) {
								if (compare(term._objValue, row.get(term._strColumnName), colType) == 0)
									vec.add(row);
							}
							if (term._strOperator.equals(">")) {
								if (compare(term._objValue, row.get(term._strColumnName), colType) > 0)
									vec.add(row);
							}
							if (term._strOperator.equals("<")) {
								if (compare(term._objValue, row.get(term._strColumnName), colType) < 0)
									vec.add(row);
							}
							if (term._strOperator.equals(">=")) {
								if (compare(term._objValue, row.get(term._strColumnName), colType) >= 0)
									vec.add(row);
							}
							if (term._strOperator.equals("<=")) {
								if (compare(term._objValue, row.get(term._strColumnName), colType) <= 0)
									vec.add(row);
							}
							if (term._strOperator.equals("!=")) {
								if (compare(term._objValue, row.get(term._strColumnName), colType) != 0)
									vec.add(row);
							}
						}

					}
				}

				vectors.add(vec);
			}

		}

		ArrayList<String> operators = new ArrayList<String>();
		for (String a : strarrOperators) {
			operators.add(a);

		}

		while (vectors.size() > 1) {
			if (operators.get(0).equals("AND") || operators.get(0).equals("and")) {

				int s = vectors.get(0).size();
				for (int i = 0; i != s; i++) {
					if (!vectors.get(1).contains(vectors.get(0).get(i))) {
						vectors.get(0).remove(vectors.get(0).get(i));
						i--;
						s = vectors.get(0).size();

					}
				}
			}
			if (operators.get(0).equals("OR") || operators.get(0).equals("or")) {
				Set<Hashtable<String, Object>> unique = new HashSet<Hashtable<String, Object>>();
				unique.addAll(vectors.get(1));
				vectors.get(0).addAll(unique);
				unique.addAll(vectors.get(0));
				vectors.get(0).removeAllElements();
				vectors.get(0).addAll(unique);

			}
			if (operators.get(0).equals("XOR") || operators.get(0).equals("xor")) {
				int s = vectors.get(0).size();
				for (int i = 0; i != s; i++) {
					if (vectors.get(1).contains(vectors.get(0).get(i))) {
						vectors.get(0).remove(vectors.get(0).get(i));
						s = vectors.get(0).size();
						i--;
					}

				}
				s = vectors.get(1).size();
				for (int i = 0; i != s; i++) {
					if (!vectors.get(0).contains(vectors.get(1).get(i))) {
						vectors.get(1).remove(vectors.get(1).get(i));
						s = vectors.get(1).size();
						i--;
					}
				}
				Set<Hashtable<String, Object>> unique = new HashSet<Hashtable<String, Object>>();
				unique.addAll(vectors.get(1));
				vectors.get(0).addAll(unique);
				unique.addAll(vectors.get(0));
				vectors.get(0).removeAllElements();
				vectors.get(0).addAll(unique);

			}
			vectors.remove(1);
			operators.remove(0);

		}
		Set<Hashtable<String, Object>> unique = new HashSet<Hashtable<String, Object>>();
		unique.addAll(vectors.get(0));
		vectors.get(0).removeAllElements();
		vectors.get(0).addAll(unique);
		return vectors.get(0).iterator();

	}

	public Iterator parseSQL(StringBuffer strbufSQL) throws Exception {
		strbufSQL = strbufSQL.append(' ');
		SQLTerm[] arrSQLTerms = null;
		String[] strarrOperators = null;
		int count = 0;
		int convert = 'A' - 'a';
		String word = "";
		boolean select = false;
		boolean from = false;
		boolean where = false;
		String tblName = "";
		for (int i = 0; i != strbufSQL.length(); i++) {
			char c = strbufSQL.charAt(i);
			if (c == ' ') {
				word = "";
			} else if (c >= 'A' && c <= 'Z') {
				c = (char) (c - convert);
				word = word + c;
			} else
				word = word + c;
			if (word.equals("or") || word.equals("xor") || word.equals("and"))
				count++;

		}
		word = "";
		int i = 0;
		for (i = 0; i != strbufSQL.length(); i++) {
			char c = strbufSQL.charAt(i);
			if (c == ' ' && from) {
				tblName = word;
				word = "";
			} else if (c == ' ') {
				word = "";
			} else
				word = word + c;
			if (word.equals("select") || word.equals("SELECT") || word.equals("Select"))
				select = true;
			if (word.equals("from") || word.equals("FROM") || word.equals("From"))
				from = true;
			if (word.equals("where") || word.equals("WHERE") || word.equals("Where")) {
				where = true;
				break;
			}
		}

		ArrayList<String> cols = new ArrayList<String>();
		ArrayList<String> colTypes = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] content = line.split(",");

			if (content[0].equals(tblName)) {

				cols.add(content[1]);
				colTypes.add(content[2]);

			}
			line = br.readLine();

		}
		br.close();
		if (where && select && from) {
			word = "";
			arrSQLTerms = new SQLTerm[count + 1];
			strarrOperators = new String[count];
			for (int a = 0; a != count + 1; a++) {
				arrSQLTerms[a] = new SQLTerm();

			}
			i = i + 2;
			for (int s = 0; s != count + 1; s++) {
				for (int j = i; j != strbufSQL.length(); j++) {
					char c = strbufSQL.charAt(j);
					if (c == ' ') {
						arrSQLTerms[s]._strTableName = tblName;
						arrSQLTerms[s]._strColumnName = word;

						if (strbufSQL.charAt(j + 2) == ' ') {
							arrSQLTerms[s]._strOperator = strbufSQL.charAt(j + 1) + "";
							i = j + 3;
						} else {
							arrSQLTerms[s]._strOperator = strbufSQL.charAt(j + 1) + "" + strbufSQL.charAt(j + 2);
							i = j + 4;
						}
						break;
					} else {
						word = word + c;
					}
				}
				word = "";
				for (int j = i; j != strbufSQL.length(); j++) {
					char c = strbufSQL.charAt(j);
					String colType = "";
					if (c == ' ') {
						for (int k = 0; k != cols.size(); k++) {
							if (cols.get(k).equals(word)) {
							}
							colType = colTypes.get(k);
						}

						arrSQLTerms[s]._objValue = cast(colType, word);

						i = j + 1;
						break;
					} else {
						word = word + c;
					}
				}
				word = "";
				if (i < strbufSQL.length())
					for (int j = i; j != strbufSQL.length(); j++) {
						char c = strbufSQL.charAt(j);
						if (c == ' ') {
							strarrOperators[s] = word;
							i = j + 1;
							break;
						} else {
							word = word + c;
						}
					}
				word = "";

			}

		}
		if (select) {
			return this.selectFromTable(arrSQLTerms, strarrOperators);
		} else
			return null;

	}

	public static String getClustKey(String strTableName) throws IOException {
		String[] clustRow = null;
		int pg;
		int numOfRows;
		ArrayList<String> cols = new ArrayList<String>();
		ArrayList<String> colTypes = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] content = line.split(",");

			if (content[3].equals("True") && content[0].equals(strTableName))
				clustRow = content;
			if (content[0].equals(strTableName)) {

				cols.add(content[1]);
				colTypes.add(content[2]);

			}
			line = br.readLine();

		}
		br.close();

		return clustRow[1];
	}

	public static String getClustKeyType(String strTableName) throws IOException {
		String[] clustRow = null;
		int pg;
		int numOfRows;
		ArrayList<String> cols = new ArrayList<String>();
		ArrayList<String> colTypes = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] content = line.split(",");

			if (content[3].equals("True") && content[0].equals(strTableName))
				clustRow = content;
			if (content[0].equals(strTableName)) {
				cols.add(content[1]);
				colTypes.add(content[2]);

			}
			line = br.readLine();

		}
		br.close();

		return clustRow[2];
	}

	public PageTable getPt() {
		return pt;
	}

	public void setPt(PageTable pt) {
		this.pt = pt;
	}

	public void printPages() throws ClassNotFoundException, IOException {
		Vector<String[]> a = new Vector(); // pagetable
		ObjectInputStream in1 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
		a = (Vector<String[]>) in1.readObject();
		in1.close();
		ObjectInputStream in = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/1.ser")));
		Vector<Hashtable<String, Object>> p = (Vector<Hashtable<String, Object>>) in.readObject();
		in.close();
		Iterator l = a.iterator();
		ArrayList<String[]> aa = new ArrayList<String[]>();
		ObjectInputStream in2 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
		a = (Vector<String[]>) in2.readObject();
		in2.close();
		while (l.hasNext()) {
			String[] k = (String[]) l.next();
			aa.add(k);
		}

		for (String[] s : aa) {
			System.out.print(s[0] + " " + s[1] + " " + s[2] + " " + s[3] + " " + s[4] + " - ");
		}
		int i = 1;
		for (Hashtable h : p) {
			System.out.print("\n" + i++ + h.entrySet());
		}
	}

	private int compare(Object a, Object b, String clustKeyType) throws ParseException {
		if (a == null)
			return 1;
		if (b == null)
			return 1;
		if (a.toString().equals("null"))
			return 1;
		if (b.toString().equals("null"))
			return -1;
		if (a.toString().equals("null") && b.toString().equals("null"))
			return 0;

		double dbl = 0;
		double dbl2 = 0;
		int intgr = 0;
		int intgr2 = 0;
		LocalDate dt = null;
		LocalDate dt2 = null;
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);
		DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("MM/dd/yyyy, hh:mm a", Locale.ENGLISH);
		DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("E MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
		if (clustKeyType.equals("java.lang.Integer")) {
			if (a instanceof String) {
				try {
					intgr = Integer.parseInt((String) a);

				} catch (Exception e) {
					return 2;
				}
			} else if (a instanceof Object) {
				intgr = (int) a;

			} else {
				intgr = (int) a;

			}
			if (b.getClass() == Integer.class)
				intgr2 = (int) b;
			else
				intgr2 = new Integer((String) b);
			if (intgr > intgr2)
				return -1;
			else if (intgr < intgr2)
				return 1;
			else
				return 0;
		}
		if (clustKeyType.equals("java.lang.Double")) {
			if (a.getClass() == Double.class)
				dbl = (double) a;
			else if (a.getClass() == String.class)
				dbl = new Double((String) a);
			if (b.getClass() == Double.class)
				dbl2 = (double) b;
			else
				dbl2 = new Double((String) b);

			if (dbl > dbl2)
				return -1;
			else if (dbl < dbl2)
				return 1;
			else
				return 0;
		}
		if (clustKeyType.equals("java.lang.String")) {
			if (((String) a.toString()).compareTo((String) b.toString()) > 0)
				return -1;
			else if (((String) a.toString()).compareTo((String) b.toString()) < 0)
				return 1;
			else
				return 0;
		}
		if (clustKeyType.equals("java.util.Date")) {

			if (b.getClass() == String.class)
				dt2 = LocalDate.parse((String) b, formatter2);

			else
				dt2 = (LocalDate) ((Date) b).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			if (a.getClass() == String.class) {
				if (((String) a).charAt(0) != 'M' && ((String) a).charAt(0) != 'T' && ((String) a).charAt(0) != 'S'
						&& ((String) a).charAt(0) != 'W' && ((String) a).charAt(0) != 'F')
					dt = LocalDate.parse((String) a, formatter);
				else
					dt = LocalDate.parse((String) a, formatter2);
			}

			else
				dt = (LocalDate) ((Date) a).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			if (dt.compareTo(dt2) > 0)
				return -1;
			else if (dt.compareTo(dt2) < 0)
				return 1;
			else
				return 0;

		}
		return 2;

	}

	private String getPage(Hashtable<String, Object> row, String strTableName) throws Exception { // complete
		Vector<String[]> pages = new Vector();
		ObjectInputStream in1 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/PageTable.ser")));
		pages = (Vector<String[]>) in1.readObject();
		in1.close();
		Iterator it = pages.iterator();
		String clustKey = getClustKey(strTableName);
		String clustKeyType = getClustKeyType(strTableName);
		Object value = row.get(clustKey);
		while (it.hasNext()) {
			String[] line = (String[]) it.next();
			if (compare(value, line[2], clustKeyType) <= 0 && compare(value, line[3], clustKeyType) >= 0) {
				return line[1];
			}
		}

		return null;

	}

	public String sort(String str1, String str2, String str3) throws Exception {
		String name = null;
		if (compare(str1, str2, "java.lang.String") > 0 && compare(str1, str3, "java.lang.String") > 0) {

			if (compare(str2, str3, "java.lang.String") > 0)
				return str1 + "." + str2 + "." + str3;
			else
				return str1 + "." + str3 + "." + str2;
		} else if (compare(str1, str3, "java.lang.String") < 0 && compare(str3, str2, "java.lang.String") > 0) {
			if (compare(str1, str2, "java.lang.String") < 0)
				return str3 + "." + str2 + "." + str1;
			else
				return str3 + "." + str1 + "." + str2;
		}
		if (compare(str1, str3, "java.lang.String") < 0)
			return str2 + "." + str3 + "." + str1;

		return str2 + "." + str1 + "." + str3;
	}
	private Hashtable getRow(Object value, String pageId, String clustKeyName, String clustKeyType) throws Exception {
		Vector<Hashtable<String, Object>> page = new Vector<Hashtable<String, Object>>();
		ObjectInputStream in1 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/" + pageId + ".ser")));
		page = (Vector<Hashtable<String, Object>>) in1.readObject();
		in1.close();
		Iterator it = page.iterator();
		while (it.hasNext()) {
			Hashtable<String, Object> row = (Hashtable<String, Object>) it.next();
			if (compare(row.get(clustKeyName), value, clustKeyType) == 0)
				return row;
		}
		System.out.println("ss");
		return null;

	}

	

}
