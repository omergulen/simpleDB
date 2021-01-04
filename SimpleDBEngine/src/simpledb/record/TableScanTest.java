package simpledb.record;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class TableScanTest {
	public static void main(String[] args) throws Exception {
		SimpleDB db = new SimpleDB("tabletest", 400, 8);
		Transaction tx = db.newTx();

		Schema sch = new Schema();
		sch.addIntField("A");
		sch.addStringField("B", 9);
		Layout layout = new Layout(sch);
		for (String fldname : layout.schema().fields()) {
			int offset = layout.offset(fldname);
			System.out.println(fldname + " has offset " + offset);
		}

		System.out.println("Filling the table with 50 random records.");
		TableScan ts = new TableScan(tx, "T", layout);
		for (int i = 0; i < 50; i++) {
			if (i % 10 == 0) {
				ts.insert();
				int n = (int) Math.round(Math.random() * 50);
				ts.setInt("A", n);
				ts.setString("B", "rec" + n);
				ts.setNull("B");
				System.out.println("inserting into slot " + ts.getRid() + ": {" + n + ", " + "rec" + n + "}");
			} else {
				ts.insert();
				int n = (int) Math.round(Math.random() * 50);
				ts.setInt("A", n);
				ts.setString("B", "rec" + n);
				System.out.println("inserting into slot " + ts.getRid() + ": {" + n + ", " + "rec" + n + "}");
			}
		}

		System.out.println("Deleting these records, whose A-values are less than 25.");
		int count = 0;
//		ts.afterLast();
//		while (ts.previous()) {
//			int a = ts.getInt("A");
//			String b = ts.getString("B");
//			if (a < 25) {
//				count++;
//				System.out.println("slot " + ts.getRid() + ": {" + a + ", " + b + "}");
//				ts.delete();
//			}
//		}
//		System.out.println(count + " values under 10 were deleted.\n");
//
//		System.out.println("Here are the remaining records.");
//		ts.afterLast();
//		while (ts.previous()) {
//			int a = ts.getInt("A");
//			String b = ts.getString("B");
//			if (ts.isNull("B"))
//				System.out.println("slot " + ts.getRid() + ": {" + a + ", " + "Null" + "}");
//			else
//				System.out.println("slot " + ts.getRid() + ": {" + a + ", " + b + "}");
//		}
		ts.close();
		tx.commit();
	}
}
