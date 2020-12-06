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
	 * called, then the calling thread will be placed on a wait list until the lock
	 * is released. If the thread remains on the wait list for a certain amount of
	 * time (currently 10 seconds), then an exception is thrown.
	 * 
	 * @param blk a reference to the disk block
	 */
	public synchronized void sLock(BlockId blk, int txid) {
		try {
			long timestamp = System.currentTimeMillis();
			while (hasXlock(blk) && !waitingTooLong(timestamp))
				wait(MAX_TIME);
			if (hasXlock(blk))
				throw new LockAbortException();
			addLock(blk, txid);
		} catch (InterruptedException e) {
			throw new LockAbortException();
		}
	}

	/**
	 * Grant an XLock on the specified block. If a lock of any type exists when the
	 * method is called, then the calling thread will be placed on a wait list until
	 * the locks are released. If the thread remains on the wait list for a certain
	 * amount of time (currently 10 seconds), then an exception is thrown.
	 * 
	 * @param blk a reference to the disk block
	 */
	synchronized void xLock(BlockId blk, int txid) {
		try {
			long timestamp = System.currentTimeMillis();
			while (hasOtherSLocks(blk) && !waitingTooLong(timestamp))
				wait(MAX_TIME);
			if (hasOtherSLocks(blk))
				throw new LockAbortException();
			addLock(blk, txid * -1);
		} catch (InterruptedException e) {
			throw new LockAbortException();
		}
	}

	/**
	 * Release a lock on the specified block. If this lock is the last lock on that
	 * block, then the waiting transactions are notified.
	 * 
	 * @param blk a reference to the disk block
	 */
	synchronized void unlock(BlockId blk, int txid) {		
		if (removeLock(blk, txid)) {
			notifyAll();
		}
	}
	
	private void addLock(BlockId blk, int txid) {
		List<Integer> blkLockList = locks.get(blk);
		if (blkLockList == null) {
			blkLockList = new ArrayList<Integer>();
			locks.put(blk, blkLockList);
		}
		blkLockList.add(txid);
	}
	
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

	private boolean hasXlock(BlockId blk) {
		List<Integer> blkLockList = locks.get(blk);
		for (Integer lock : blkLockList) {
			if (lock < 0) {
				return true;
			}
		}

		return false;
	}

	private boolean hasOtherSLocks(BlockId blk) {
		List<Integer> blkLockList = locks.get(blk);
		for (Integer lock : blkLockList) {
			if (lock > 0) {
				return true;
			}
		}

		return false;
	}

	private boolean waitingTooLong(long starttime) {
		return System.currentTimeMillis() - starttime > MAX_TIME;
	}
}
