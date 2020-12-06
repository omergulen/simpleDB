package simpledb.tx.recovery;

import java.util.ArrayList;
import java.util.List;

import simpledb.file.Page;
import simpledb.log.LogMgr;
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

	public void undo(Transaction tx) {
	}

	public List<Integer> getTxs() {
		return txs;
	}


	/**
	 * A static method to write a checkpoint record to the log. This log record
	 * contains the NQCKPT operator, and active txids.
	 * 
	 * @return the LSN of the last log value
	 */
	public static int writeToLog(LogMgr lm, List<Integer> txs) {
		int txCount = txs.size();
		int totalCount = txCount + 2;
		byte[] rec = new byte[totalCount * Integer.BYTES];
		Page p = new Page(rec);
		p.setInt(RECORD_TYPE_OFFSET, NQCKPT);
		p.setInt(TX_COUNT_OFFSET, txCount);
		for (int i = 0; i < txCount; i++) {
			int txId = txs.get(i);
			int offset = (i + 2) * Integer.BYTES;
			p.setInt(offset, txId);
		}
		return lm.append(rec);
	}

}
