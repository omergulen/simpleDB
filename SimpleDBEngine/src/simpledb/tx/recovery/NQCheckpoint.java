package simpledb.tx.recovery;

import simpledb.tx.Transaction;

public class NQCheckpoint implements LogRecord {

	public NQCheckpoint() {
	}

	@Override
	public int op() {
		return NQCKPT;
	}

   /**
    * Checkpoint records have no associated transaction,
    * and so the method returns a "dummy", negative txid.
    */
   public int txNumber() {
      return -1; // dummy value
   }

	@Override
	public void undo(Transaction tx) {}

}
