package simpledb.query;

public class UnionScan implements Scan {

	private Scan s1;
	private Scan s2;
	private Scan currentScan;

	/**
	 * Create a project scan having the specified underlying scan and field list.
	 * 
	 * @param s         the underlying scan
	 * @param fieldlist the list of field names
	 */
	public UnionScan(Scan s1, Scan s2) {
		this.s1 = s1;
		this.s2 = s2;
		currentScan = s1;
	}

	@Override
	public void beforeFirst() {
		s1.beforeFirst();
		s2.beforeFirst();
	}

	@Override
	public boolean next() {
		if (s1.next()) {
			return true;
		} else {
			setCurrentScan(s2);
			return s2.next();
		}
	}

	@Override
	public int getInt(String fldname) {
		return currentScan.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		return currentScan.getString(fldname);
	}

	@Override
	public Constant getVal(String fldname) {
		return currentScan.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return s1.hasField(fldname);
	}

	@Override
	public void close() {
		s1.close();
		s2.close();
	}

	private void setCurrentScan(Scan cS) {
		currentScan = cS;
	}

}
