package simpledb.tx.recovery;

import java.util.ArrayList;
import java.util.List;

import simpledb.file.Page;
import simpledb.tx.Transaction;

public class NQCheckpoint implements LogRecord {

	private List<Integer> txs;
	private final static int RECORD_TYPE_OFFSET = 0;
	private final static int TX_COUNT_OFFSET = Integer.BYTES;

	public NQCheckpoint(Page p) {
		int txCount = p.getInt(TX_COUNT_OFFSET);
		txs = new ArrayList<Integer>();
		for (int i = 0; i < txCount; i++) {
			int offset = (i + 2) * Integer.BYTES;
			txs.add(p.getInt(offset));
		}
	}

	public int op() {
		return NQCKPT;
	}

	/**
	 * Checkpoint records have no associated transaction, and so the method returns
	 * a "dummy", negative txid.
	 */
	public int txNumber() {
		return -1; // dummy value
	}

	@Override
	public void undo(Transaction tx) {}

}
