package Octree;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Vector;

public class Leaf extends Node {
	private Object[] arr;
	private int numOfEntries;
	private int size;

	public Leaf(Range range, Position position, int size) {
		super(range, position);
		this.size = size;
		arr = new Object[size];
		numOfEntries = 0;

	}

	public Object[] getArr() {
		return arr;
	}

	public void addEntry(Object entry, String clustKeyName, String clustKeyType, String col1, 
			String col2, String col3,String col1Type, String col2Type, String col3Type) throws Exception {
		Hashtable<String, Object> ent = new Hashtable();
		if (entry instanceof Entry)
			ent = getRow(((Entry) entry).getValue(), ((Entry) entry).getPageId(), clustKeyName, clustKeyType);
		else if (entry instanceof LinkedList)
			ent = getRow(((Entry) ((LinkedList) entry).get(0)).getValue(),
					((Entry) ((LinkedList) entry).get(0)).getPageId(), clustKeyName, clustKeyType);
		
		boolean added = false;
		for (int i = 0; i != numOfEntries; i++) {
			
			Hashtable<String, Object> row = new Hashtable<String, Object>();
			
			if (arr[i] instanceof Entry)
				row = getRow(((Entry) arr[i]).getValue(), ((Entry) arr[i]).getPageId(), clustKeyName, clustKeyType);
			else if (arr[i] instanceof LinkedList)
				row = getRow(((Entry) ((LinkedList) arr[i]).get(0)).getValue(),
						((Entry) ((LinkedList) arr[i]).get(0)).getPageId(), clustKeyName, clustKeyType);
			if(row==null)
				System.out.print(((Entry) arr[i]).getPageId() + "-"+ ((Entry) arr[i]).getValue());
			if (compare(ent.get(col1), row.get(col1), col1Type) == 0
					&& compare(ent.get(col2), row.get(col2), col2Type) == 0
					&& compare(ent.get(col3), row.get(col3), col3Type) == 0) {
				
				if (arr[i] instanceof Entry) {
					LinkedList<Entry> entLL = new LinkedList();
					entLL.add((Entry) entry);
					entLL.add((Entry) arr[i]);
					arr[i]=entLL;
					added = true;
					break;

				} else if (arr[i] instanceof LinkedList) {
					
					((LinkedList) arr[i]).add(entry);
					added = true;
					break;
				} else
					throw new Exception("Entry of type" + arr[i].getClass());
				
			}
			
		}
		
		if (!added) {
			arr[numOfEntries] = entry;
			numOfEntries++;
		}
	}

	public void removeEntry(Object value, String clustKeyType) throws ParseException {

		boolean shift = false; // start shifting item found
		int old = arr.length;
		boolean emptyLL = false;
		boolean ent = false;
		for (int i = 0; i != numOfEntries; i++) {
			if (arr[i] instanceof Entry)
				if (compare(((Entry) arr[i]).getValue(), value, clustKeyType) == 0) {
					arr[i] = null;
					ent = true;
					numOfEntries--;
					break;
				}
			if (arr[i] instanceof LinkedList) {
				int size = ((LinkedList) arr[i]).size();
				for (int k = 0; k != size; k++) {
					if (compare(((Entry) ((LinkedList) arr[i]).get(k)).getValue(), value, clustKeyType) == 0) {
						((LinkedList) arr[i]).remove(((LinkedList) arr[i]).get(k));
						if (((LinkedList) arr[i]).size() == 0)
						numOfEntries--;

					}
				}
			}
		}
		int i = 0;
		for (i = 0; i != numOfEntries+1; i++) {
			if (shift)
				arr[i - 1] = arr[i];
			if (arr[i] == null)
				shift = true;

		}
		if (emptyLL || ent)
			arr[i - 1] = null;

		
			

	}

	public void setArr(Entry[] arr) {
		this.arr = arr;
	}

	public int getNumOfEntries() {
		return numOfEntries;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public boolean isEmpty() {
		if (numOfEntries == 0)
			return true;
		return false;
	}

	public boolean isFull() {
		if (numOfEntries == size)
			return true;
		return false;
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
			} else
				intgr = (int) a;
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

	private Hashtable getRow(Object value, String pageId, String clustKeyName, String clustKeyType) throws Exception {
		Vector<Hashtable<String, Object>> page = new Vector<Hashtable<String, Object>>();
		
		ObjectInputStream in1 = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream("src/main/resources/data/" + pageId + ".ser")));
		page = (Vector<Hashtable<String, Object>>) in1.readObject();
		in1.close();
		Iterator it = page.iterator();
		while (it.hasNext()) {
			Hashtable<String, Object> row = (Hashtable<String, Object>) it.next();
			if (compare(row.get(clustKeyName), value, clustKeyType) == 0) {
				
				return row;
			}
			
		}
		
		
		return null;

	}
}
