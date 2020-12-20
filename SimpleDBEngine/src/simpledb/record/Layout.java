package simpledb.record;

import java.util.*;
import static java.sql.Types.*;
import simpledb.file.Page;

/**
 * Description of the structure of a record. It contains the name, type, length
 * and offset of each field of the table.
 * 
 * @author Edward Sciore
 *
 */
public class Layout {
	private Schema schema;
	private Map<String, Integer> offsets;
	private Map<String, Integer> bitPositions;
	private int slotsize;

	/**
	 * This constructor creates a Layout object from a schema. This constructor is
	 * used when a table is created. It determines the physical offset of each field
	 * within the record.
	 * 
	 * @param tblname the name of the table
	 * @param schema  the schema of the table's records
	 */
	public Layout(Schema schema) {
		this.schema = schema;
		offsets = new HashMap<>();
		bitPositions = new HashMap<>();
		int pos = Integer.BYTES; // leave space for the empty/inuse flag
		int bitPosition = 1;
		for (String fldname : schema.fields()) {
			offsets.put(fldname, pos);
			bitPositions.put(fldname, bitPosition);
			pos += lengthInBytes(fldname);
			bitPosition++;
		}
		slotsize = pos;
	}

	/**
	 * Create a Layout object from the specified metadata. This constructor is used
	 * when the metadata is retrieved from the catalog.
	 * 
	 * @param tblname   the name of the table
	 * @param schema    the schema of the table's records
	 * @param offsets   the already-calculated offsets of the fields within a record
	 * @param recordlen the already-calculated length of each record
	 */
	public Layout(Schema schema, Map<String, Integer> offsets, int slotsize) {
		this.schema = schema;
		this.offsets = offsets;
		this.slotsize = slotsize;
		int bitPosition = 1;
		for (String fldname : schema.fields()) {
			bitPositions.put(fldname, bitPosition);
			bitPosition++;
		}
	}

	/**
	 * Return the schema of the table's records
	 * 
	 * @return the table's record schema
	 */
	public Schema schema() {
		return schema;
	}

	/**
	 * Return the offset of a specified field within a record
	 * 
	 * @param fldname the name of the field
	 * @return the offset of that field within a record
	 */
	public int offset(String fldname) {
		return offsets.get(fldname);
	}
	
	public int bitPosition(String fldname) {
		return bitPositions.get(fldname);
	}

	/**
	 * Return the size of a slot, in bytes.
	 * 
	 * @return the size of a slot
	 */
	public int slotSize() {
		return slotsize;
	}

	private int lengthInBytes(String fldname) {
		int fldtype = schema.type(fldname);
		if (fldtype == INTEGER)
			return Integer.BYTES;
		else // fldtype == VARCHAR
			return Page.maxLength(schema.length(fldname));
	}
}
