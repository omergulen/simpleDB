package simpledb.tx.concurrency;

import java.util.*;
import simpledb.file.BlockId;

/**
 * The lock table, which provides methods to lock and unlock blocks. If a
 * transaction requests a lock that causes a conflict with an existing lock,
 * then that transaction is placed on a wait list. There is only one wait list
 * for all blocks. When the last lock on a block is unlocked, then all
 * transactions are removed from the wait list and rescheduled. If one of those
 * transactions discovers that the lock it is waiting for is still locked, it
 * will place itself back on the wait list.
 * 
 * @author Edward Sciore
 */
class LockTable {

	private Map<BlockId, List<Integer>> locks = new HashMap<BlockId, List<Integer>>();

	/**
	 * Grant an SLock on the specified block. If an XLock exists when the method is
	 * called, if the calling thread is younger than the thread has XLock on the block
	 * then an exception will be thrown. The calling thread (older) will wait until 
	 * the lock is released.
	 * 
	 * @param blk a reference to the disk block
	 * @param txid id of the transaction
	 */
	public synchronized void sLock(BlockId blk, int txid) {

		if (shouldAbortThreadForSLock(blk, txid))
			throw new LockAbortException();

		try {
			while (hasXlock(blk))
				wait(Long.MAX_VALUE);
		} catch (InterruptedException e) {
			throw new LockAbortException();
		}
		addLock(blk, txid);
	}

	/**
	 * Grant an XLock on the specified block. If a lock of any type exists when the
	 * method is called, if the calling thread is younger than the thread has XLock on the block
	 * then an exception will be thrown. The calling thread (older) will wait until 
	 * the lock is released.
	 * 
	 * @param blk a reference to the disk block
	 * @param txid id of the transaction
	 * 
	 */
	synchronized void xLock(BlockId blk, int txid) {
		if (shouldAbortThreadForXLock(blk, txid))
			throw new LockAbortException();

		try {
			while (hasXlock(blk))
				wait(Long.MAX_VALUE);
		} catch (InterruptedException e) {
			throw new LockAbortException();
		}

		addLock(blk, txid * -1);
	}

	/**
	 * Release a lock on the specified block. If this lock is the last lock on that
	 * block, then the waiting transactions are notified.
	 * 
	 * @param blk a reference to the disk block
	 * @param txid id of the transaction
	 */
	synchronized void unlock(BlockId blk, int txid) {
		if (removeLock(blk, txid)) {
			notifyAll();
		}
	}

	/**
	 * Release a lock on the specified block. If this lock is the last lock on that
	 * block, then the waiting transactions are notified.
	 * Overloading the original method for back compatibility.
	 * 
	 * @param blk a reference to the disk block
	 */
	synchronized void unlock(BlockId blk) {
		if (removeLock(blk)) {
			notifyAll();
		}
	}

	/**
	 * Private helper method to add locks on blocks.
	 * If txid is negative, it is xLock.
	 * 
	 * @param blk a reference to the disk block
	 * @param txid id of the transaction
	 */
	private void addLock(BlockId blk, int txid) {
		List<Integer> blkLockList = locks.get(blk);
		if (blkLockList == null) {
			blkLockList = new ArrayList<Integer>();
			locks.put(blk, blkLockList);
		}
		blkLockList.add(txid);
	}

	/**
	 * Private helper method to remove locks from blocks.
	 * 
	 * @param blk a reference to the disk block
	 * @param txid id of the transaction
	 */
	private boolean removeLock(BlockId blk, int txid) {
		List<Integer> blkLockList = locks.get(blk);
		if (blkLockList != null) {
			blkLockList.remove(txid);
		}

		if (blkLockList == null || blkLockList.isEmpty()) {
			locks.remove(blk);
			return true;
		}

		return false;
	}

	/**
	 * Private helper method to remove locks from blocks.
	 * Overloading the original method for back compatibility.
	 * 
	 * @param blk a reference to the disk block
	 */
	private boolean removeLock(BlockId blk) {
		locks.remove(blk);
		return true;
	}

	private boolean hasXlock(BlockId blk) {
		List<Integer> blkLockList = locks.get(blk);

		if (blkLockList == null) {
			return false;
		}

		for (Integer lock : blkLockList) {
			if (lock < 0) {
				return true;
			}
		}

		return false;
	}

	private boolean shouldAbortThreadForSLock(BlockId blk, int txid) {
		return shouldAbortThread(blk, txid, false);
	}

	private boolean shouldAbortThreadForXLock(BlockId blk, int txid) {
		return shouldAbortThread(blk, txid, true);
	}

	private boolean shouldAbortThread(BlockId blk, int txid, boolean xLock) {
		List<Integer> blkLockList = locks.get(blk);
		if (blkLockList == null) {
			return false;
		}

		txid = Math.abs(txid);
		for (Integer lockedTxid : blkLockList) {
			if (((!xLock && lockedTxid < 0) || xLock) && Math.abs(lockedTxid) < txid) {
				return true;
			}
		}

		return false;
	}
}
