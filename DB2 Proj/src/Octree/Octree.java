package Octree;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Set;
import java.util.Vector;

import javax.swing.text.DateFormatter;

public class Octree implements java.io.Serializable {
	private Node root;
	private Octree l1;
	private Octree l2; // node
	private Octree l3; // l1 //l2 //l3 //l4 //r1 //r2 //r3 //r4
	private Octree l4;
	private Octree r1;
	private Octree r2;
	private Octree r3;
	private Octree r4;
	public int numOfEntries;

	public Octree(Node root, Octree l1, Octree l2, Octree l3, Octree l4, Octree r1, Octree r2, Octree r3, Octree r4) {
		this.root = root;
		this.l1 = l1;
		this.l2 = l2;
		this.l3 = l3;
		this.l4 = l4;
		this.r1 = r1;
		this.r2 = r2;
		this.r3 = r3;
		this.r4 = r4;
		numOfEntries = 0;
	}

	public void create(String pageName, String col1Type, Object min1, Object max1, String col2Type, Object min2,
			Object max2, String col3Type, Object min3, Object max3, int entPerNode) throws Exception {
		Object mid1 = div(col1Type, min1, max1);
		Object mid2 = div(col2Type, min2, max2);
		Object mid3 = div(col3Type, min3, max3);
		Range r = new Range(min1, max1, min2, max2, min3, max3);
		NLeaf newRoot = new NLeaf(r, Position.ROOT);
		Range newRange1 = new Range(min1, mid1, min2, mid2, min3, mid3); // 0-0.5,0-0.5,0-0.5
		Range newRange2 = new Range(mid1, max1, min2, mid2, min3, mid3); // 0.5-1,0-0.5,0-0.5
		Range newRange3 = new Range(min1, mid1, mid2, max2, min3, mid3); // 0-0.5,0.5-1,0-0.5
		Range newRange4 = new Range(min1, mid1, min2, mid2, mid3, max3); // 0-0.5,0-0.5,0.5-1
		Range newRange5 = new Range(mid1, max1, mid2, max2, min3, mid3); // 0.5-1,0.5-1,0-0.5
		Range newRange6 = new Range(mid1, max1, min2, mid2, mid3, max3); // 0.5-1,0-0.5,0.5-1
		Range newRange7 = new Range(min1, mid1, mid2, max2, mid3, max3); // 0-0.5,0.5-1,0.5-1
		Range newRange8 = new Range(mid1, max1, mid2, max2, mid3, max3); // 0.5-1,0.5-1,0.5-1

		Leaf newL1 = new Leaf(newRange1, Position.L1, entPerNode);
		Leaf newL2 = new Leaf(newRange2, Position.L2, entPerNode);
		Leaf newL3 = new Leaf(newRange3, Position.L3, entPerNode);
		Leaf newL4 = new Leaf(newRange4, Position.L4, entPerNode);
		Leaf newR1 = new Leaf(newRange5, Position.R1, entPerNode);
		Leaf newR2 = new Leaf(newRange6, Position.R2, entPerNode);
		Leaf newR3 = new Leaf(newRange7, Position.R3, entPerNode);
		Leaf newR4 = new Leaf(newRange8, Position.R4, entPerNode);
		Octree oct1 = new Octree(newL1, null, null, null, null, null, null, null, null);
		Octree oct2 = new Octree(newL2, null, null, null, null, null, null, null, null);
		Octree oct3 = new Octree(newL3, null, null, null, null, null, null, null, null);
		Octree oct4 = new Octree(newL4, null, null, null, null, null, null, null, null);
		Octree oct5 = new Octree(newR1, null, null, null, null, null, null, null, null);
		Octree oct6 = new Octree(newR2, null, null, null, null, null, null, null, null);
		Octree oct7 = new Octree(newR3, null, null, null, null, null, null, null, null);
		Octree oct8 = new Octree(newR4, null, null, null, null, null, null, null, null);

		Octree main = new Octree(newRoot, oct1, oct2, oct3, oct4, oct5, oct6, oct7, oct8);

		PrintWriter writer = new PrintWriter("src/main/resources/data/" + pageName + ".ser");
		writer.print(""); // clear page
		writer.close();

		ObjectOutputStream out1 = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream("src/main/resources/data/" + pageName + ".ser")));
		try {
			out1.writeObject(main);
		} finally {
			out1.close();
		}

	}

	public void insert(Entry entry, String col1Name, Object col1Value, String col1Type, String col2Name,
			Object col2Value, String col2Type, String col3Name, Object col3Value, String col3Type, String clustKeyName,
			String clustKeyType) throws Exception {
		Octree parent = this;
		Octree child = this;
		Node current = root;

		if (col1Value != null && col2Value != null && col3Value != null)
			while (true) {

				if (((compare(col1Value, parent.l1.getRoot().getRange().getMin1(), col1Type) <= 0
						&& compare(col1Value, parent.l1.getRoot().getRange().getMax1(), col1Type) > 0)
						|| col1Value.toString().equals("null"))
						&& ((compare(col2Value, parent.l1.getRoot().getRange().getMin2(), col2Type) <= 0
								&& compare(col2Value, parent.l1.getRoot().getRange().getMax2(), col2Type) > 0)
								|| col2Value.toString().equals("null"))
						&& ((compare(col3Value, parent.l1.getRoot().getRange().getMin3(), col3Type) <= 0
								&& compare(col3Value, parent.l1.getRoot().getRange().getMax3(), col3Type) > 0))
						|| col3Value.toString().equals("null")) { // checking l1

					current = parent.getL1().getRoot();
					child = parent.getL1();
					if (current instanceof Leaf) {

						if (((Leaf) current).getSize() > ((Leaf) current).getNumOfEntries()) { // array not full
							((Leaf) current).addEntry(entry, clustKeyName, clustKeyType, col1Name, col2Name, col3Name,
									col1Type, col2Type, col3Type);
							break;
						} else {// leaf is full

							child = child.split(col1Name, col1Type, col2Name, col2Type, col3Name, col3Type,
									clustKeyName, clustKeyType);
							parent.setL1(child); // parent pointer
							parent = parent.getL1(); // parent now is the new Octree
						}
					} else {
						parent = parent.getL1();
					}

				}

				if (((compare(col1Value, parent.l2.getRoot().getRange().getMin1(), col1Type) <= 0
						&& compare(col1Value, parent.l2.getRoot().getRange().getMax1(), col1Type) >= 0))
						&& ((compare(col2Value, parent.l2.getRoot().getRange().getMin2(), col2Type) <= 0
								&& compare(col2Value, parent.l2.getRoot().getRange().getMax2(), col2Type) > 0)
								|| col2Value.toString().equals("null"))
						&& ((compare(col3Value, parent.l2.getRoot().getRange().getMin3(), col3Type) <= 0
								&& compare(col3Value, parent.l2.getRoot().getRange().getMax3(), col3Type) > 0)
								|| col3Value.toString().equals("null"))) { // checking l1
					current = parent.getL2().getRoot();
					child = parent.getL2();
					if (current instanceof Leaf) {

						if (((Leaf) current).getSize() > ((Leaf) current).getNumOfEntries()) { // array not full
							((Leaf) current).addEntry(entry, clustKeyName, clustKeyType, col1Name, col2Name, col3Name,
									col1Type, col2Type, col3Type);
							numOfEntries++;
							break;
						} else {// leaf is full
							child = child.split(col1Name, col1Type, col2Name, col2Type, col3Name, col3Type,
									clustKeyName, clustKeyType);
							parent.setL2(child); // parent pointer
							parent = child; // parent now is the NLeaf
						}
					} else
						parent = parent.getL2();

				}

				if (((compare(col1Value, parent.l3.getRoot().getRange().getMin1(), col1Type) <= 0
						&& compare(col1Value, parent.l3.getRoot().getRange().getMax1(), col1Type) > 0)
						|| col1Value.toString().equals("null"))
						&& ((compare(col2Value, parent.l3.getRoot().getRange().getMin2(), col2Type) <= 0
								&& compare(col2Value, parent.l3.getRoot().getRange().getMax2(), col2Type) >= 0))
						&& ((compare(col3Value, parent.l3.getRoot().getRange().getMin3(), col3Type) <= 0
								&& compare(col3Value, parent.l3.getRoot().getRange().getMax3(), col3Type) > 0))
						|| col3Value.toString().equals("null")) { // checking l1

					current = parent.getL3().getRoot();
					child = parent.getL3();
					if (current instanceof Leaf) {

						if (((Leaf) current).getSize() > ((Leaf) current).getNumOfEntries()) { // array not full
							((Leaf) current).addEntry(entry, clustKeyName, clustKeyType, col1Name, col2Name, col3Name,
									col1Type, col2Type, col3Type);
							numOfEntries++;
							break;
						} else {// leaf is full

							child = child.split(col1Name, col1Type, col2Name, col2Type, col3Name, col3Type,
									clustKeyName, clustKeyType);
							parent.setL3(child); // parent pointer
							parent = child; // parent now is the NLeaf
						}
					} else
						parent = parent.getL3();

				}

				if (((compare(col1Value, parent.l4.getRoot().getRange().getMin1(), col1Type) <= 0
						&& compare(col1Value, parent.l4.getRoot().getRange().getMax1(), col1Type) > 0)
						|| col1Value.toString().equals("null"))
						&& compare(col2Value, parent.l4.getRoot().getRange().getMin2(), col2Type) <= 0
						&& compare(col2Value, parent.l4.getRoot().getRange().getMax2(), col2Type) > 0
						&& compare(col3Value, parent.l4.getRoot().getRange().getMin3(), col3Type) <= 0
						&& compare(col3Value, parent.l4.getRoot().getRange().getMax3(), col3Type) >= 0) { // checking l1
					current = parent.getL4().getRoot();
					child = parent.getL4();
					if (current instanceof Leaf) {

						if (((Leaf) current).getSize() > ((Leaf) current).getNumOfEntries()) { // array not full
							((Leaf) current).addEntry(entry, clustKeyName, clustKeyType, col1Name, col2Name, col3Name,
									col1Type, col2Type, col3Type);
							numOfEntries++;
							break;
						} else {// leaf is full
							child = child.split(col1Name, col1Type, col2Name, col2Type, col3Name, col3Type,
									clustKeyName, clustKeyType);
							parent.setL4(child); // parent pointer
							parent = child; // parent now is the NLeaf
						}
					} else
						parent = parent.l4;

				}

				if (compare(col1Value, parent.r1.getRoot().getRange().getMin1(), col1Type) <= 0
						&& compare(col1Value, parent.r1.getRoot().getRange().getMax1(), col1Type) >= 0
						&& compare(col2Value, parent.r1.getRoot().getRange().getMin2(), col2Type) <= 0
						&& compare(col2Value, parent.r1.getRoot().getRange().getMax2(), col2Type) >= 0
						&& ((compare(col3Value, parent.r1.getRoot().getRange().getMin3(), col3Type) <= 0
								&& compare(col3Value, parent.r1.getRoot().getRange().getMax3(), col3Type) > 0)
								|| col3Value.toString().equals("null"))) { // checking l1
					current = parent.getR1().getRoot();
					child = parent.getR1();
					if (current instanceof Leaf) {

						if (((Leaf) current).getSize() > ((Leaf) current).getNumOfEntries()) { // array not full
							((Leaf) current).addEntry(entry, clustKeyName, clustKeyType, col1Name, col2Name, col3Name,
									col1Type, col2Type, col3Type);
							numOfEntries++;
							break;
						} else {// leaf is full
							child = child.split(col1Name, col1Type, col2Name, col2Type, col3Name, col3Type,
									clustKeyName, clustKeyType);
							parent.setR1(child); // parent pointer
							parent = child; // parent now is the NLeaf
						}
					} else
						parent = parent.r1;

				}

				if (compare(col1Value, parent.r2.getRoot().getRange().getMin1(), col1Type) <= 0
						&& compare(col1Value, parent.r2.getRoot().getRange().getMax1(), col1Type) >= 0
						&& ((compare(col2Value, parent.r2.getRoot().getRange().getMin2(), col2Type) <= 0
								&& compare(col2Value, parent.r2.getRoot().getRange().getMax2(), col2Type) > 0)
								|| col2Value.toString().equals("null"))
						&& compare(col3Value, parent.r2.getRoot().getRange().getMin3(), col3Type) <= 0
						&& compare(col3Value, parent.r2.getRoot().getRange().getMax3(), col3Type) >= 0) { // checking l1
					current = parent.getR2().getRoot();
					child = parent.getR2();
					if (current instanceof Leaf) {

						if (((Leaf) current).getSize() > ((Leaf) current).getNumOfEntries()) { // array not full
							((Leaf) current).addEntry(entry, clustKeyName, clustKeyType, col1Name, col2Name, col3Name,
									col1Type, col2Type, col3Type);
							numOfEntries++;
							break;
						} else {// leaf is full
							child = child.split(col1Name, col1Type, col2Name, col2Type, col3Name, col3Type,
									clustKeyName, clustKeyType);
							parent.setR2(child); // parent pointer
							parent = child; // parent now is the NLeaf
						}
					} else
						parent = parent.r2;

				}

				if (((compare(col1Value, parent.r3.getRoot().getRange().getMin1(), col1Type) <= 0
						&& compare(col1Value, parent.r3.getRoot().getRange().getMax1(), col1Type) > 0)
						|| col1Value.toString().equals("null"))
						&& compare(col2Value, parent.r3.getRoot().getRange().getMin2(), col2Type) <= 0
						&& compare(col2Value, parent.r3.getRoot().getRange().getMax2(), col2Type) >= 0
						&& compare(col3Value, parent.r3.getRoot().getRange().getMin3(), col3Type) <= 0
						&& compare(col3Value, parent.r3.getRoot().getRange().getMax3(), col3Type) >= 0) { // checking l1
					current = parent.getR3().getRoot();
					child = parent.getR3();
					if (current instanceof Leaf) {

						if (((Leaf) current).getSize() > ((Leaf) current).getNumOfEntries()) { // array not full
							((Leaf) current).addEntry(entry, clustKeyName, clustKeyType, col1Name, col2Name, col3Name,
									col1Type, col2Type, col3Type);
							numOfEntries++;
							break;
						} else {// leaf is full
							child = child.split(col1Name, col1Type, col2Name, col2Type, col3Name, col3Type,
									clustKeyName, clustKeyType);
							parent.setR3(child); // parent pointer
							parent = child; // parent now is the NLeaf
						}
					} else
						parent = parent.r3;

				}
				if (compare(col1Value, parent.r4.getRoot().getRange().getMin1(), col1Type) <= 0
						&& compare(col1Value, parent.r4.getRoot().getRange().getMax1(), col1Type) >= 0
						&& compare(col2Value, parent.r4.getRoot().getRange().getMin2(), col2Type) <= 0
						&& compare(col2Value, parent.r4.getRoot().getRange().getMax2(), col2Type) >= 0
						&& compare(col3Value, parent.r4.getRoot().getRange().getMin3(), col3Type) <= 0
						&& compare(col3Value, parent.r4.getRoot().getRange().getMax3(), col3Type) >= 0) { // checking l1
					current = parent.getR4().getRoot();
					child = parent.getR4();
					if (current instanceof Leaf) {

						if (((Leaf) current).getSize() > ((Leaf) current).getNumOfEntries()) { // array not full
							((Leaf) current).addEntry(entry, clustKeyName, clustKeyType, col1Name, col2Name, col3Name,
									col1Type, col2Type, col3Type);
							numOfEntries++;
							break;
						} else {// leaf is full
							child = child.split(col1Name, col1Type, col2Name, col2Type, col3Name, col3Type,
									clustKeyName, clustKeyType);
							parent.setR4(child); // parent pointer
							parent = child; // parent now is the NLeaf
						}
					} else
						parent = parent.r4;

				}

			}
	}

	private Octree split(String col1Name, String col1Type, String col2Name, String col2Type, String col3Name,
			String col3Type, String clustKeyName, String clustKeyType) throws Exception {
		// create leaves <- root of 8 octrees
		// divide all values by 2
		// return new Nleaf octree with 8 octree with leaves as roots

		Leaf oldRoot = (Leaf) this.getRoot();
		Object[] oldArr = oldRoot.getArr();
		Object min1 = oldRoot.getRange().getMin1();
		Object min2 = oldRoot.getRange().getMin2();
		Object min3 = oldRoot.getRange().getMin3();
		Object max1 = oldRoot.getRange().getMax1();
		Object max2 = oldRoot.getRange().getMax2();
		Object max3 = oldRoot.getRange().getMax3();

		Object mid1 = div(col1Type, min1, max1);
		Object mid2 = div(col2Type, min2, max2);
		Object mid3 = div(col3Type, min3, max3);

		NLeaf newRoot = new NLeaf(oldRoot.getRange(), oldRoot.getPosition());
		Range newRange1 = new Range(min1, mid1, min2, mid2, min3, mid3); // 0-0.5,0-0.5,0-0.5
		Range newRange2 = new Range(mid1, max1, min2, mid2, min3, mid3); // 0.5-1,0-0.5,0-0.5
		Range newRange3 = new Range(min1, mid1, mid2, max2, min3, mid3); // 0-0.5,0.5-1,0-0.5
		Range newRange4 = new Range(min1, mid1, min2, mid2, mid3, max3); // 0-0.5,0-0.5,0.5-1
		Range newRange5 = new Range(mid1, max1, mid2, max2, min3, mid3); // 0.5-1,0.5-1,0-0.5
		Range newRange6 = new Range(mid1, max1, min2, mid2, mid3, max3); // 0.5-1,0-0.5,0.5-1
		Range newRange7 = new Range(min1, mid1, mid2, max2, mid3, max3); // 0-0.5,0.5-1,0.5-1
		Range newRange8 = new Range(mid1, max1, mid2, max2, mid3, max3); // 0.5-1,0.5-1,0.5-1

		Leaf newL1 = new Leaf(newRange1, Position.L1, oldRoot.getSize());
		Leaf newL2 = new Leaf(newRange2, Position.L2, oldRoot.getSize());
		Leaf newL3 = new Leaf(newRange3, Position.L3, oldRoot.getSize());
		Leaf newL4 = new Leaf(newRange4, Position.L4, oldRoot.getSize());
		Leaf newR1 = new Leaf(newRange5, Position.R1, oldRoot.getSize());
		Leaf newR2 = new Leaf(newRange6, Position.R2, oldRoot.getSize());
		Leaf newR3 = new Leaf(newRange7, Position.R3, oldRoot.getSize());
		Leaf newR4 = new Leaf(newRange8, Position.R4, oldRoot.getSize());

		for (int i = 0; i != oldRoot.getNumOfEntries(); i++) { // add entries to leaves
			Hashtable currentRow = new Hashtable();

			if (oldArr[i] instanceof Entry) {
				currentRow = getRow(((Entry) oldArr[i]).getValue(), ((Entry) oldArr[i]).getPageId(), clustKeyName,
						clustKeyType);
			} else if (oldArr[i] instanceof LinkedList) {
				currentRow = getRow(((Entry) ((LinkedList) oldArr[i]).get(0)).getValue(),
						((Entry) ((LinkedList) oldArr[i]).get(0)).getPageId(), clustKeyName, clustKeyType);

			}

			if (compare(currentRow.get(col1Name), newL1.getRange().getMin1(), col1Type) <= 0
					&& compare(currentRow.get(col1Name), newL1.getRange().getMax1(), col1Type) > 0
					&& compare(currentRow.get(col2Name), newL1.getRange().getMin2(), col2Type) <= 0
					&& compare(currentRow.get(col2Name), newL1.getRange().getMax2(), col2Type) > 0
					&& compare(currentRow.get(col3Name), newL1.getRange().getMin3(), col3Type) <= 0
					&& compare(currentRow.get(col3Name), newL1.getRange().getMax3(), col3Type) > 0) {
				newL1.addEntry(oldArr[i], clustKeyName, clustKeyType, col1Name, col2Name, col3Name, col1Type, col2Type,
						col3Type);

			}
			if (compare(currentRow.get(col1Name), newL2.getRange().getMin1(), col1Type) <= 0
					&& compare(currentRow.get(col1Name), newL2.getRange().getMax1(), col1Type) >= 0
					&& compare(currentRow.get(col2Name), newL2.getRange().getMin2(), col2Type) <= 0
					&& compare(currentRow.get(col2Name), newL2.getRange().getMax2(), col2Type) > 0
					&& compare(currentRow.get(col3Name), newL2.getRange().getMin3(), col3Type) <= 0
					&& compare(currentRow.get(col3Name), newL2.getRange().getMax3(), col3Type) > 0) {

				newL2.addEntry(oldArr[i], clustKeyName, clustKeyType, col1Name, col2Name, col3Name, col1Type, col2Type,
						col3Type);

			}

			if (compare(currentRow.get(col1Name), newL3.getRange().getMin1(), col1Type) <= 0
					&& compare(currentRow.get(col1Name), newL3.getRange().getMax1(), col1Type) > 0
					&& compare(currentRow.get(col2Name), newL3.getRange().getMin2(), col2Type) <= 0
					&& compare(currentRow.get(col2Name), newL3.getRange().getMax2(), col2Type) >= 0
					&& compare(currentRow.get(col3Name), newL3.getRange().getMin3(), col3Type) <= 0
					&& compare(currentRow.get(col3Name), newL3.getRange().getMax3(), col3Type) > 0) {
				newL3.addEntry(oldArr[i], clustKeyName, clustKeyType, col1Name, col2Name, col3Name, col1Type, col2Type,
						col3Type);

			}

			if (compare(currentRow.get(col1Name), newL4.getRange().getMin1(), col1Type) <= 0
					&& compare(currentRow.get(col1Name), newL4.getRange().getMax1(), col1Type) > 0
					&& compare(currentRow.get(col2Name), newL4.getRange().getMin2(), col2Type) <= 0
					&& compare(currentRow.get(col2Name), newL4.getRange().getMax2(), col2Type) > 0
					&& compare(currentRow.get(col3Name), newL4.getRange().getMin3(), col3Type) <= 0
					&& compare(currentRow.get(col3Name), newL4.getRange().getMax3(), col3Type) >= 0) {
				newL4.addEntry(oldArr[i], clustKeyName, clustKeyType, col1Name, col2Name, col3Name, col1Type, col2Type,
						col3Type);

			}

			if (compare(currentRow.get(col1Name), newR1.getRange().getMin1(), col1Type) <= 0
					&& compare(currentRow.get(col1Name), newR1.getRange().getMax1(), col1Type) >= 0
					&& compare(currentRow.get(col2Name), newR1.getRange().getMin2(), col2Type) <= 0
					&& compare(currentRow.get(col2Name), newR1.getRange().getMax2(), col2Type) >= 0
					&& compare(currentRow.get(col3Name), newR1.getRange().getMin3(), col3Type) <= 0
					&& compare(currentRow.get(col3Name), newR1.getRange().getMax3(), col3Type) > 0) {
				newR1.addEntry(oldArr[i], clustKeyName, clustKeyType, col1Name, col2Name, col3Name, col1Type, col2Type,
						col3Type);
			}

			if (compare(currentRow.get(col1Name), newR2.getRange().getMin1(), col1Type) <= 0
					&& compare(currentRow.get(col1Name), newR2.getRange().getMax1(), col1Type) >= 0
					&& compare(currentRow.get(col2Name), newR2.getRange().getMin2(), col2Type) <= 0
					&& compare(currentRow.get(col2Name), newR2.getRange().getMax2(), col2Type) > 0
					&& compare(currentRow.get(col3Name), newR2.getRange().getMin3(), col3Type) <= 0
					&& compare(currentRow.get(col3Name), newR2.getRange().getMax3(), col3Type) >= 0) {
				newR2.addEntry(oldArr[i], clustKeyName, clustKeyType, col1Name, col2Name, col3Name, col1Type, col2Type,
						col3Type);
			}

			if (compare(currentRow.get(col1Name), newR3.getRange().getMin1(), col1Type) <= 0
					&& compare(currentRow.get(col1Name), newR3.getRange().getMax1(), col1Type) > 0
					&& compare(currentRow.get(col2Name), newR3.getRange().getMin2(), col2Type) <= 0
					&& compare(currentRow.get(col2Name), newR3.getRange().getMax2(), col2Type) >= 0
					&& compare(currentRow.get(col3Name), newR3.getRange().getMin3(), col3Type) <= 0
					&& compare(currentRow.get(col3Name), newR3.getRange().getMax3(), col3Type) >= 0) {
				newR3.addEntry(oldArr[i], clustKeyName, clustKeyType, col1Name, col2Name, col3Name, col1Type, col2Type,
						col3Type);
			}

			if (compare(currentRow.get(col1Name), newR4.getRange().getMin1(), col1Type) <= 0
					&& compare(currentRow.get(col1Name), newR4.getRange().getMax1(), col1Type) >= 0
					&& compare(currentRow.get(col2Name), newR4.getRange().getMin2(), col2Type) <= 0
					&& compare(currentRow.get(col2Name), newR4.getRange().getMax2(), col2Type) >= 0
					&& compare(currentRow.get(col3Name), newR4.getRange().getMin3(), col3Type) <= 0
					&& compare(currentRow.get(col3Name), newR4.getRange().getMax3(), col3Type) >= 0) {
				newR4.addEntry(oldArr[i], clustKeyName, clustKeyType, col1Name, col2Name, col3Name, col1Type, col2Type,
						col3Type);

			}

		}

		Octree oct1 = new Octree(newL1, null, null, null, null, null, null, null, null);
		Octree oct2 = new Octree(newL2, null, null, null, null, null, null, null, null);
		Octree oct3 = new Octree(newL3, null, null, null, null, null, null, null, null);
		Octree oct4 = new Octree(newL4, null, null, null, null, null, null, null, null);
		Octree oct5 = new Octree(newR1, null, null, null, null, null, null, null, null);
		Octree oct6 = new Octree(newR2, null, null, null, null, null, null, null, null);
		Octree oct7 = new Octree(newR3, null, null, null, null, null, null, null, null);
		Octree oct8 = new Octree(newR4, null, null, null, null, null, null, null, null);

		return new Octree(newRoot, oct1, oct2, oct3, oct4, oct5, oct6, oct7, oct8);
	}

	private static Object div(String colType, Object min, Object max) throws ParseException {
		if (colType.equals("java.lang.Integer")) {
			int minInt = (int) min;
			int maxInt = (int) max;

			return (int) ((maxInt + minInt) / 2);
		} else if (colType.equals("java.lang.Double")) {
			double minInt = (Double) min;
			double maxInt = (Double) max;

			return (double) ((maxInt + minInt) / 2.0);
		} else if (colType.equals("java.lang.String")) {
			String maxStr = (String) max;
			String minStr = (String) min;
			String val = "";
			for (int i = 0; i != maxStr.length(); i++) {
				if (i < minStr.length())
					val = val + (char) ((maxStr.charAt(i) + (minStr.charAt(i))) / 2);
				else
					val = val + (char) ((maxStr.charAt(i)) / 2);
			}
			return val;
		} else if (colType.equals("java.util.Date")) {
			LocalDate maxDt = (LocalDate) ((Date) max).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			LocalDate minDt = (LocalDate) ((Date) min).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			int midD = (maxDt.getDayOfMonth() + minDt.getDayOfMonth()) / 2;
			int midM = (maxDt.getMonthValue() + minDt.getMonthValue()) / 2;
			int midY = (maxDt.getYear() + minDt.getYear()) / 2;
			String strDt = midY + "-" + midM + "-" + midD;
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Date dt = formatter.parse(strDt);

			return dt;
		}

		return null;

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

	public Vector<Entry> delete(Vector<Object> primVals, String clustKeyType) throws Exception {
		if (primVals.size() == 0)
			return null;
		return delTuple(this, primVals, clustKeyType, new Vector());

	}

	private Vector<Entry> delTuple(Octree tree, Vector<Object> primVals, String clustKeyType, Vector<Entry> finalpageId)
			throws Exception {

		if (tree.getRoot() instanceof Leaf) {

			int s = ((Leaf) tree.getRoot()).getNumOfEntries();
			for (int i = 0; i != s; i++) {

				Object[] arr = ((Leaf) tree.getRoot()).getArr();
				if (arr[i] != null)
					if (arr[i] instanceof Entry) {
						if (primVals.remove(((Entry) arr[i]).getValue())) {
							finalpageId.add((Entry) arr[i]);
							((Leaf) tree.getRoot()).removeEntry(((Entry) arr[i]).getValue(), clustKeyType);
							i--;

						}

					} else if (arr[i] instanceof LinkedList) {
						int k = ((LinkedList) arr[i]).size();
						for (int l = 0; l != k; l++) {
							if (k < ((LinkedList) arr[i]).size())
								if (primVals.remove(((Entry) ((LinkedList) arr[i]).get(k)).getValue())) {
									finalpageId.add(((Entry) ((LinkedList) arr[i]).get(k)));
									((Leaf) tree.getRoot()).removeEntry(
											((Entry) ((LinkedList) arr[i]).get(k)).getValue(), clustKeyType);
									i = 0;

									k = ((LinkedList) arr[i]).size();

								}
						}

						s = ((Leaf) tree.getRoot()).getNumOfEntries();
					}
			}
		} else {
			Vector<Entry> s = delTuple(tree.getL1(), primVals, clustKeyType, finalpageId);
			if (s != null)
				finalpageId.addAll(s);
			s = delTuple(tree.getL2(), primVals, clustKeyType, finalpageId);
			if (s != null)
				finalpageId.addAll(s);
			s = delTuple(tree.getL3(), primVals, clustKeyType, finalpageId);
			if (s != null)
				finalpageId.addAll(s);
			s = delTuple(tree.getL4(), primVals, clustKeyType, finalpageId);
			if (s != null)
				finalpageId.addAll(s);
			s = delTuple(tree.getR1(), primVals, clustKeyType, finalpageId);
			if (s != null)
				finalpageId.addAll(s);
			s = delTuple(tree.getR2(), primVals, clustKeyType, finalpageId);
			if (s != null)
				finalpageId.addAll(s);
			s = delTuple(tree.getR3(), primVals, clustKeyType, finalpageId);
			if (s != null)
				finalpageId.addAll(s);
			s = delTuple(tree.getR4(), primVals, clustKeyType, finalpageId);
			if (s != null)
				finalpageId.addAll(s);

		}
		Set<Entry> unique = new HashSet<Entry>();
		unique.addAll(finalpageId);
		finalpageId.removeAllElements();
		finalpageId.addAll(unique);
		return finalpageId;
	}

	public void update(Object clustKeyValue, Entry entry, String col1Name, Object col1Value, String col1Type,
			String col2Name, Object col2Value, String col2Type, String col3Name, Object col3Value, String col3Type,
			String clustKeyName, String clustKeyType) throws Exception {
		Vector<Object> primVal = new Vector();
		primVal.add(clustKeyValue);
		delete(primVal, clustKeyType);
		insert(entry, col1Name, col1Value, col1Type, col2Name, col2Value, col2Type, col3Name, col3Value, col3Type,
				clustKeyName, clustKeyType);
	}

	public Node getRoot() {
		return root;
	}

	public void setRoot(Node root) {
		this.root = root;
	}

	public Octree getL1() {
		return l1;
	}

	public void setL1(Octree l1) {
		this.l1 = l1;
	}

	public Octree getL2() {
		return l2;
	}

	public void setL2(Octree l2) {
		this.l2 = l2;
	}

	public Octree getL3() {
		return l3;
	}

	public void setL3(Octree l3) {
		this.l3 = l3;
	}

	public Octree getL4() {
		return l4;
	}

	public void setL4(Octree l4) {
		this.l4 = l4;
	}

	public Octree getR1() {
		return r1;
	}

	public void setR1(Octree r1) {
		this.r1 = r1;
	}

	public Octree getR2() {
		return r2;
	}

	public void setR2(Octree r2) {
		this.r2 = r2;
	}

	public Octree getR3() {
		return r3;
	}

	public void setR3(Octree r3) {
		this.r3 = r3;
	}

	public Octree getR4() {
		return r4;
	}

	public void setR4(Octree r4) {
		this.r4 = r4;
	}

	public void updatePageId(Entry entry, String col1Name, Object col1Value, String col1Type, String col2Name,
			Object col2Value, String col2Type, String col3Name, Object col3Value, String col3Type, String clustKeyName,
			String clustKeyType) throws ParseException {
		if (root instanceof Leaf) {
			Object[] arr = ((Leaf) root).getArr();
			for (Object ent : arr) {
				if (ent instanceof Entry) {
					if (compare(((Entry) ent).getValue(), entry.getValue(), clustKeyType) == 0) {
						((Entry) ent).setPageId(entry.getPageId());
					}
				}
				if (ent instanceof LinkedList) {
					for (Object entr : (LinkedList) ent) {
						if (compare(((Entry) entr).getValue(), entry.getValue(), clustKeyType) == 0) {
							((Entry) entr).setPageId(entry.getPageId());

						}
					}
				}
			}

		} else {
			if (((compare(col1Value, l1.getRoot().getRange().getMin1(), col1Type) <= 0
					&& compare(col1Value, l1.getRoot().getRange().getMax1(), col1Type) > 0)
					|| col1Value.toString().equals("null"))
					&& ((compare(col2Value, l1.getRoot().getRange().getMin2(), col2Type) <= 0
							&& compare(col2Value, l1.getRoot().getRange().getMax2(), col2Type) > 0)
							|| col2Value.toString().equals("null"))
					&& ((compare(col3Value, l1.getRoot().getRange().getMin3(), col3Type) <= 0
							&& compare(col3Value, l1.getRoot().getRange().getMax3(), col3Type) > 0))
					|| col3Value.toString().equals("null")) { // checking l1

				l1.updatePageId(entry, col1Name, col1Value, col1Type, col2Name, col2Value, col2Type, col3Name,
						col3Value, col3Type, clustKeyName, clustKeyType);

			}

			if (((compare(col1Value, l2.getRoot().getRange().getMin1(), col1Type) <= 0
					&& compare(col1Value, l2.getRoot().getRange().getMax1(), col1Type) >= 0))
					&& ((compare(col2Value, l2.getRoot().getRange().getMin2(), col2Type) <= 0
							&& compare(col2Value, l2.getRoot().getRange().getMax2(), col2Type) > 0)
							|| col2Value.toString().equals("null"))
					&& ((compare(col3Value, l2.getRoot().getRange().getMin3(), col3Type) <= 0
							&& compare(col3Value, l2.getRoot().getRange().getMax3(), col3Type) > 0)
							|| col3Value.toString().equals("null"))) { // checking l1
				l2.updatePageId(entry, col1Name, col1Value, col1Type, col2Name, col2Value, col2Type, col3Name,
						col3Value, col3Type, clustKeyName, clustKeyType);
			}

			if (((compare(col1Value, l3.getRoot().getRange().getMin1(), col1Type) <= 0
					&& compare(col1Value, l3.getRoot().getRange().getMax1(), col1Type) > 0)
					|| col1Value.toString().equals("null"))
					&& ((compare(col2Value, l3.getRoot().getRange().getMin2(), col2Type) <= 0
							&& compare(col2Value, l3.getRoot().getRange().getMax2(), col2Type) >= 0))
					&& ((compare(col3Value, l3.getRoot().getRange().getMin3(), col3Type) <= 0
							&& compare(col3Value, l3.getRoot().getRange().getMax3(), col3Type) > 0))
					|| col3Value.toString().equals("null")) { // checking l1
				l3.updatePageId(entry, col1Name, col1Value, col1Type, col2Name, col2Value, col2Type, col3Name,
						col3Value, col3Type, clustKeyName, clustKeyType);
			}

			if (((compare(col1Value, l4.getRoot().getRange().getMin1(), col1Type) <= 0
					&& compare(col1Value, l4.getRoot().getRange().getMax1(), col1Type) > 0)
					|| col1Value.toString().equals("null"))
					&& compare(col2Value, l4.getRoot().getRange().getMin2(), col2Type) <= 0
					&& compare(col2Value, l4.getRoot().getRange().getMax2(), col2Type) > 0
					&& compare(col3Value, l4.getRoot().getRange().getMin3(), col3Type) <= 0
					&& compare(col3Value, l4.getRoot().getRange().getMax3(), col3Type) >= 0) { // checking l1
				l4.updatePageId(entry, col1Name, col1Value, col1Type, col2Name, col2Value, col2Type, col3Name,
						col3Value, col3Type, clustKeyName, clustKeyType);
			}

			if (compare(col1Value, r1.getRoot().getRange().getMin1(), col1Type) <= 0
					&& compare(col1Value, r1.getRoot().getRange().getMax1(), col1Type) >= 0
					&& compare(col2Value, r1.getRoot().getRange().getMin2(), col2Type) <= 0
					&& compare(col2Value, r1.getRoot().getRange().getMax2(), col2Type) >= 0
					&& ((compare(col3Value, r1.getRoot().getRange().getMin3(), col3Type) <= 0
							&& compare(col3Value, r1.getRoot().getRange().getMax3(), col3Type) > 0)
							|| col3Value.toString().equals("null"))) { // checking l1
				r1.updatePageId(entry, col1Name, col1Value, col1Type, col2Name, col2Value, col2Type, col3Name,
						col3Value, col3Type, clustKeyName, clustKeyType);
			}

			if (compare(col1Value, r2.getRoot().getRange().getMin1(), col1Type) <= 0
					&& compare(col1Value, r2.getRoot().getRange().getMax1(), col1Type) >= 0
					&& ((compare(col2Value, r2.getRoot().getRange().getMin2(), col2Type) <= 0
							&& compare(col2Value, r2.getRoot().getRange().getMax2(), col2Type) > 0)
							|| col2Value.toString().equals("null"))
					&& compare(col3Value, r2.getRoot().getRange().getMin3(), col3Type) <= 0
					&& compare(col3Value, r2.getRoot().getRange().getMax3(), col3Type) >= 0) { // checking l1
				r2.updatePageId(entry, col1Name, col1Value, col1Type, col2Name, col2Value, col2Type, col3Name,
						col3Value, col3Type, clustKeyName, clustKeyType);
			}

			if (((compare(col1Value, r3.getRoot().getRange().getMin1(), col1Type) <= 0
					&& compare(col1Value, r3.getRoot().getRange().getMax1(), col1Type) > 0)
					|| col1Value.toString().equals("null"))
					&& compare(col2Value, r3.getRoot().getRange().getMin2(), col2Type) <= 0
					&& compare(col2Value, r3.getRoot().getRange().getMax2(), col2Type) >= 0
					&& compare(col3Value, r3.getRoot().getRange().getMin3(), col3Type) <= 0
					&& compare(col3Value, r3.getRoot().getRange().getMax3(), col3Type) >= 0) { // checking l1
				r3.updatePageId(entry, col1Name, col1Value, col1Type, col2Name, col2Value, col2Type, col3Name,
						col3Value, col3Type, clustKeyName, clustKeyType);
			}
			if (compare(col1Value, r4.getRoot().getRange().getMin1(), col1Type) <= 0
					&& compare(col1Value, r4.getRoot().getRange().getMax1(), col1Type) >= 0
					&& compare(col2Value, r4.getRoot().getRange().getMin2(), col2Type) <= 0
					&& compare(col2Value, r4.getRoot().getRange().getMax2(), col2Type) >= 0
					&& compare(col3Value, r4.getRoot().getRange().getMin3(), col3Type) <= 0
					&& compare(col3Value, r4.getRoot().getRange().getMax3(), col3Type) >= 0) { // checking l1
				r4.updatePageId(entry, col1Name, col1Value, col1Type, col2Name, col2Value, col2Type, col3Name,
						col3Value, col3Type, clustKeyName, clustKeyType);
			}
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
					System.out.println("catch int");
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
				try {
					dbl2 = new Double((String) b);
				} catch (Exception e) {

				}

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
				if (((String) b).charAt(0) != 'M' && ((String) b).charAt(0) != 'T' && ((String) b).charAt(0) != 'S'
						&& ((String) b).charAt(0) != 'W' && ((String) b).charAt(0) != 'F')
					dt2 = LocalDate.parse((String) b, formatter);
				else
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

	public Vector<Hashtable<String, Object>> selectEqual(String colName, String colType, Object value, int colNum,
			String clustKey, String clustKeyType, Vector<Hashtable<String, Object>> finalVec) throws Exception {

		if (this.root instanceof Leaf) {

			for (int i = 0; i != ((Leaf) this.root).getNumOfEntries(); i++) {
				Hashtable curr = new Hashtable();
				if (((Leaf) this.root).getArr()[i] instanceof Entry)
					curr = getRow(((Entry) ((Leaf) this.root).getArr()[i]).getValue(),
							((Entry) ((Leaf) this.root).getArr()[i]).getPageId(), clustKey, clustKeyType);
				else if (((Leaf) this.root).getArr()[i] instanceof LinkedList)
					curr = getRow(((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(0)).getValue(),
							((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(0)).getPageId(), clustKey,
							clustKeyType);
				if (curr == null) {
					System.out.println(((Entry) ((Leaf) this.root).getArr()[i]).getValue());
				}
				if(curr!=null) 
				if (compare(curr.get(colName), value, colType) == 0) {
					if (((Leaf) this.root).getArr()[i] instanceof Entry)
						finalVec.add(curr);
					else
						for (int l = 0; l != ((LinkedList) ((Leaf) this.root).getArr()[i]).size(); l++)
							finalVec.add(
									getRow(((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(l)).getValue(),
											((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(l)).getPageId(),
											clustKey, clustKeyType));
				}

			}

		}

		if (this.root instanceof NLeaf) {
			if (colNum == 1) {
				if (compare(value, this.l1.getRoot().getRange().getMin1(), colType) <= 0
						&& compare(value, this.l1.getRoot().getRange().getMax1(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l1.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, this.l2.getRoot().getRange().getMin1(), colType) <= 0
						&& compare(value, this.l2.getRoot().getRange().getMax1(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = l2.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, this.l3.getRoot().getRange().getMin1(), colType) <= 0
						&& compare(value, this.l3.getRoot().getRange().getMax1(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l3.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l4.getRoot().getRange().getMin1(), colType) <= 0
						&& compare(value, l4.getRoot().getRange().getMax1(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l4.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r1.getRoot().getRange().getMin1(), colType) <= 0
						&& compare(value, r1.getRoot().getRange().getMax1(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r1.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, r2.getRoot().getRange().getMin1(), colType) <= 0
						&& compare(value, r2.getRoot().getRange().getMax1(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r2.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r3.getRoot().getRange().getMin1(), colType) <= 0
						&& compare(value, r3.getRoot().getRange().getMax1(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = r3.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);

				}
				if (compare(value, r4.getRoot().getRange().getMin1(), colType) <= 0
						&& compare(value, r4.getRoot().getRange().getMax1(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r4.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

			}
			if (colNum == 2) {
				if (compare(value, this.l1.getRoot().getRange().getMin2(), colType) <= 0
						&& compare(value, this.l1.getRoot().getRange().getMax2(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l1.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l2.getRoot().getRange().getMin2(), colType) <= 0
						&& compare(value, l2.getRoot().getRange().getMax2(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l2.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l3.getRoot().getRange().getMin2(), colType) <= 0
						&& compare(value, l3.getRoot().getRange().getMax2(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = l3.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l4.getRoot().getRange().getMin2(), colType) <= 0
						&& compare(value, l4.getRoot().getRange().getMax2(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l4.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r1.getRoot().getRange().getMin2(), colType) <= 0
						&& compare(value, r1.getRoot().getRange().getMax2(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r1.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r2.getRoot().getRange().getMin2(), colType) <= 0
						&& compare(value, r2.getRoot().getRange().getMax2(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = r2.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r3.getRoot().getRange().getMin2(), colType) <= 0
						&& compare(value, r3.getRoot().getRange().getMax2(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r3.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r4.getRoot().getRange().getMin2(), colType) <= 0
						&& compare(value, r4.getRoot().getRange().getMax2(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r4.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}
			}
			if (colNum == 3) {

				if (compare(value, this.l1.getRoot().getRange().getMin3(), colType) <= 0
						&& compare(value, this.l1.getRoot().getRange().getMax3(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l1.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l2.getRoot().getRange().getMin3(), colType) <= 0
						&& compare(value, l2.getRoot().getRange().getMax3(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l2.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l3.getRoot().getRange().getMin3(), colType) <= 0
						&& compare(value, l3.getRoot().getRange().getMax3(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l3.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, l4.getRoot().getRange().getMin3(), colType) <= 0
						&& compare(value, l4.getRoot().getRange().getMax3(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = l4.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r1.getRoot().getRange().getMin3(), colType) <= 0
						&& compare(value, r1.getRoot().getRange().getMax3(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = r1.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r2.getRoot().getRange().getMin3(), colType) <= 0
						&& compare(value, r2.getRoot().getRange().getMax3(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r2.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r3.getRoot().getRange().getMin3(), colType) <= 0
						&& compare(value, r3.getRoot().getRange().getMax3(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r3.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, r4.getRoot().getRange().getMin3(), colType) <= 0
						&& compare(value, r4.getRoot().getRange().getMax3(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r4.selectEqual(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

			}
		}
		Set<Hashtable<String, Object>> unique = new HashSet<Hashtable<String, Object>>();
		unique.addAll(finalVec);
		finalVec.removeAllElements();
		finalVec.addAll(unique);
		return finalVec;

	}

	public Vector<Hashtable<String, Object>> selectLessThan(String colName, String colType, Object value, int colNum,
			String clustKey, String clustKeyType, Vector<Hashtable<String, Object>> finalVec) throws Exception {

		if (this.root instanceof Leaf) {

			for (int i = 0; i != ((Leaf) this.root).getNumOfEntries(); i++) {
				Hashtable curr = new Hashtable();
				if (((Leaf) this.root).getArr()[i] instanceof Entry)
					curr = getRow(((Entry) ((Leaf) this.root).getArr()[i]).getValue(),
							((Entry) ((Leaf) this.root).getArr()[i]).getPageId(), clustKey, clustKeyType);
				else if (((Leaf) this.root).getArr()[i] instanceof LinkedList)
					curr = getRow(((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(0)).getValue(),
							((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(0)).getPageId(), clustKey,
							clustKeyType);
				if (curr != null)
					if (compare(curr.get(colName), value, colType) > 0) {

						if (((Leaf) this.root).getArr()[i] instanceof Entry)
							finalVec.add(curr);
						else if (((Leaf) this.root).getArr()[i] instanceof LinkedList)
							for (int l = 0; l != ((LinkedList) ((Leaf) this.root).getArr()[i]).size(); l++)
								finalVec.add(getRow(
										((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(l)).getValue(),
										((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(l)).getPageId(),
										clustKey, clustKeyType));
					}

			}

		}

		if (this.root instanceof NLeaf) {
			if (colNum == 1) {
				if (compare(value, this.l1.getRoot().getRange().getMin1(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l1.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, this.l2.getRoot().getRange().getMin1(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l2.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, this.l3.getRoot().getRange().getMin1(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l3.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l4.getRoot().getRange().getMin1(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l4.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r1.getRoot().getRange().getMin1(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r1.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, r2.getRoot().getRange().getMin1(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r2.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r3.getRoot().getRange().getMin1(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r3.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);

				}
				if (compare(value, r4.getRoot().getRange().getMin1(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r4.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

			}
			if (colNum == 2) {
				if (compare(value, this.l1.getRoot().getRange().getMin2(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l1.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l2.getRoot().getRange().getMin2(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l2.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l3.getRoot().getRange().getMin2(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l3.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l4.getRoot().getRange().getMin2(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l4.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r1.getRoot().getRange().getMin2(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r1.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r2.getRoot().getRange().getMin2(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r2.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r3.getRoot().getRange().getMin2(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r3.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r4.getRoot().getRange().getMin2(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r4.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}
			}
			if (colNum == 3) {
				if (compare(value, this.l1.getRoot().getRange().getMin3(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l1.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l2.getRoot().getRange().getMin3(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l2.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l3.getRoot().getRange().getMin3(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l3.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, l4.getRoot().getRange().getMin3(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = l4.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r1.getRoot().getRange().getMin3(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r1.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r2.getRoot().getRange().getMin3(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r2.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r3.getRoot().getRange().getMin3(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r3.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, r4.getRoot().getRange().getMin3(), colType) <= 0) {
					Vector<Hashtable<String, Object>> s = r4.selectLessThan(colName, colType, value, colNum, clustKey,
							clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

			}
		}
		Set<Hashtable<String, Object>> unique = new HashSet<Hashtable<String, Object>>();
		unique.addAll(finalVec);
		finalVec.removeAllElements();
		finalVec.addAll(unique);
		return finalVec;

	}

	public Vector<Hashtable<String, Object>> selectAll(String clustKey, String clustKeyType,
			Vector<Hashtable<String, Object>> finalVec) throws Exception {

		if (this.root instanceof Leaf) {

			for (int i = 0; i != ((Leaf) this.root).getNumOfEntries(); i++) {
				Hashtable curr = new Hashtable();
				if (((Leaf) this.root).getArr()[i] instanceof Entry)
					curr = getRow(((Entry) ((Leaf) this.root).getArr()[i]).getValue(),
							((Entry) ((Leaf) this.root).getArr()[i]).getPageId(), clustKey, clustKeyType);
				else if (((Leaf) this.root).getArr()[i] instanceof LinkedList)
					curr = getRow(((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(0)).getValue(),
							((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(0)).getPageId(), clustKey,
							clustKeyType);
				if(curr!=null) 
				if (((Leaf) this.root).getArr()[i] instanceof Entry)
					finalVec.add(curr);
				else
					for (int l = 0; l != ((LinkedList) ((Leaf) this.root).getArr()[i]).size(); l++)
						finalVec.add(getRow(((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(l)).getValue(),
								((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(l)).getPageId(), clustKey,
								clustKeyType));
			}

		}

		if (this.root instanceof NLeaf) {

			Vector<Hashtable<String, Object>> s = l1.selectAll(clustKey, clustKeyType, finalVec);

			if (s != null)
				finalVec.addAll(s);
			s = l2.selectAll(clustKey, clustKeyType, finalVec);

			if (s != null)
				finalVec.addAll(s);
			s = l3.selectAll(clustKey, clustKeyType, finalVec);

			if (s != null)
				finalVec.addAll(s);
			s = l4.selectAll(clustKey, clustKeyType, finalVec);

			if (s != null)
				finalVec.addAll(s);
			s = r1.selectAll(clustKey, clustKeyType, finalVec);

			if (s != null)
				finalVec.addAll(s);
			s = r2.selectAll(clustKey, clustKeyType, finalVec);

			if (s != null)
				finalVec.addAll(s);
			s = r3.selectAll(clustKey, clustKeyType, finalVec);

			if (s != null)
				finalVec.addAll(s);
			s = r4.selectAll(clustKey, clustKeyType, finalVec);

			if (s != null)
				finalVec.addAll(s);
		}
		int s = finalVec.size();
		for (int i = 0; i != s; i++) {
			Hashtable curr = finalVec.get(i);
			finalVec.remove(i);
			if (finalVec.contains(curr)) {
				i--;
				s = finalVec.size();
			} else
				finalVec.add(curr);

		}
		Set<Hashtable<String, Object>> unique = new HashSet<Hashtable<String, Object>>();
		unique.addAll(finalVec);
		finalVec.removeAllElements();
		finalVec.addAll(unique);
		return finalVec;

	}

	public Vector<Hashtable<String, Object>> selectGreaterThan(String colName, String colType, Object value, int colNum,
			String clustKey, String clustKeyType, Vector<Hashtable<String, Object>> finalVec) throws Exception {

		if (this.root instanceof Leaf) {
			for (int i = 0; i != ((Leaf) this.root).getNumOfEntries(); i++) {
				Hashtable curr = new Hashtable();
				if (((Leaf) this.root).getArr()[i] instanceof Entry)
					curr = getRow(((Entry) ((Leaf) this.root).getArr()[i]).getValue(),
							((Entry) ((Leaf) this.root).getArr()[i]).getPageId(), clustKey, clustKeyType);
				else if (((Leaf) this.root).getArr()[i] instanceof LinkedList)
					curr = getRow(((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(0)).getValue(),
							((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(0)).getPageId(), clustKey,
							clustKeyType);
				if(curr!=null) 
				if (compare(curr.get(colName), value, colType) < 0) {

					if (((Leaf) this.root).getArr()[i] instanceof Entry) {
						finalVec.add(curr);
					} else if (((Leaf) this.root).getArr()[i] instanceof LinkedList)
						for (int l = 0; l != ((LinkedList) ((Leaf) this.root).getArr()[i]).size(); l++)
							finalVec.add(
									getRow(((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(l)).getValue(),
											((Entry) ((LinkedList) ((Leaf) this.root).getArr()[i]).get(l)).getPageId(),
											clustKey, clustKeyType));
				}

			}

		}

		if (this.root instanceof NLeaf) {
			if (colNum == 1) {
				if (compare(value, this.l1.getRoot().getRange().getMax1(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l1.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, this.l2.getRoot().getRange().getMax1(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = l2.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, this.l3.getRoot().getRange().getMax1(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l3.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l4.getRoot().getRange().getMax1(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l4.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r1.getRoot().getRange().getMax1(), colType) >= 0) {

					Vector<Hashtable<String, Object>> s = r1.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, r2.getRoot().getRange().getMax1(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r2.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r3.getRoot().getRange().getMax1(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = r3.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);

					if (s != null)
						finalVec.addAll(s);

				}
				if (compare(value, r4.getRoot().getRange().getMax1(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r4.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

			}
			if (colNum == 2) {
				if (compare(value, this.l1.getRoot().getRange().getMax2(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l1.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l2.getRoot().getRange().getMax2(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l2.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l3.getRoot().getRange().getMax2(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = l3.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l4.getRoot().getRange().getMax2(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l4.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r1.getRoot().getRange().getMax2(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r1.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r2.getRoot().getRange().getMax2(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = r2.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r3.getRoot().getRange().getMax2(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r3.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r4.getRoot().getRange().getMax2(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r4.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}
			}
			if (colNum == 3) {
				if (compare(value, this.l1.getRoot().getRange().getMax3(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l1.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l2.getRoot().getRange().getMax3(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l2.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, l3.getRoot().getRange().getMax3(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = l3.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, l4.getRoot().getRange().getMax3(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = l4.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r1.getRoot().getRange().getMax3(), colType) > 0) {
					Vector<Hashtable<String, Object>> s = r1.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r2.getRoot().getRange().getMax3(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r2.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

				if (compare(value, r3.getRoot().getRange().getMax3(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r3.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}
				if (compare(value, r4.getRoot().getRange().getMax3(), colType) >= 0) {
					Vector<Hashtable<String, Object>> s = r4.selectGreaterThan(colName, colType, value, colNum,
							clustKey, clustKeyType, finalVec);
					if (s != null)
						finalVec.addAll(s);
				}

			}
		}
		Set<Hashtable<String, Object>> unique = new HashSet<Hashtable<String, Object>>();
		unique.addAll(finalVec);
		finalVec.removeAllElements();
		finalVec.addAll(unique);
		return finalVec;

	}

	public Vector<Hashtable<String, Object>> selectLessThanEqual(String colName, String colType, Object value,
			int colNum, String clustKey, String clustKeyType, Vector<Hashtable<String, Object>> finalVec)
			throws Exception {
		Vector<Hashtable<String, Object>> lessThan = selectLessThan(colName, colType, value, colNum, clustKey,
				clustKeyType, finalVec);
		Vector<Hashtable<String, Object>> equal = selectEqual(colName, colType, value, colNum, clustKey, clustKeyType,
				finalVec);
		finalVec.addAll(lessThan);
		finalVec.addAll(equal);
		Set<Hashtable<String, Object>> unique = new HashSet<Hashtable<String, Object>>();
		unique.addAll(finalVec);
		finalVec.removeAllElements();
		finalVec.addAll(unique);
		return finalVec;

	}

	public Vector<Hashtable<String, Object>> selectGreaterThanEqual(String colName, String colType, Object value,
			int colNum, String clustKey, String clustKeyType, Vector<Hashtable<String, Object>> finalVec)
			throws Exception {
		Vector<Hashtable<String, Object>> greaterThan = selectGreaterThan(colName, colType, value, colNum, clustKey,
				clustKeyType, finalVec);
		Vector<Hashtable<String, Object>> equal = selectEqual(colName, colType, value, colNum, clustKey, clustKeyType,
				finalVec);
		finalVec.addAll(greaterThan);
		finalVec.addAll(equal);
		Set<Hashtable<String, Object>> unique = new HashSet<Hashtable<String, Object>>();
		unique.addAll(finalVec);
		finalVec.removeAllElements();
		finalVec.addAll(unique);
		return finalVec;

	}

	public void printTree() {
		if (root instanceof Leaf) {
			if (((Leaf) root).getNumOfEntries() != 0)
				System.out.println("numOfEntries: " + ((Leaf) root).getNumOfEntries());
		} else {
			l1.printTree();
			l2.printTree();
			l3.printTree();
			l4.printTree();
			r1.printTree();
			r2.printTree();
			r3.printTree();
			r4.printTree();
		}
	}


}
