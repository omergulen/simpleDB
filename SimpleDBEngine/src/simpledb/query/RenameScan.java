package simpledb.query;

public class RenameScan implements Scan {

	private Scan s;
	private String oldFieldName;
	private String newFieldName;

	public RenameScan(Scan s, String oldFieldName, String newFieldName) {
		this.s = s;
		this.oldFieldName = oldFieldName;
		this.newFieldName = newFieldName;
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();
	}

	@Override
	public boolean next() {
		return s.next();
	}

	@Override
	public int getInt(String fldname) {
		String fieldName = overrideFldnameIfNecessary(fldname);
		return s.getInt(fieldName);
	}

	@Override
	public String getString(String fldname) {
		String fieldName = overrideFldnameIfNecessary(fldname);
		return s.getString(fieldName);
	}

	@Override
	public Constant getVal(String fldname) {
		String fieldName = overrideFldnameIfNecessary(fldname);
		return s.getVal(fieldName);
	}

	@Override
	public boolean hasField(String fldname) {
		String fieldName = overrideFldnameIfNecessary(fldname);
		return s.hasField(fieldName);
	}

	@Override
	public void close() {
		s.close();
	}

	private String overrideFldnameIfNecessary(String fldname) {
		if (fldname.equals(newFieldName)) {
			return oldFieldName;
		}

		return fldname;
	}

}
