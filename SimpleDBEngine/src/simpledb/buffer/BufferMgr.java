package simpledb.buffer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import simpledb.file.*;
import simpledb.log.LogMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 *
 */
public class BufferMgr {
	private List<Buffer> unpinnedBuffers;
	private Map<BlockId, Buffer> allocatedBuffers;
	private int numAvailable;
	private static final long MAX_TIME = 10000; // 10 seconds

	/**
	 * Creates a buffer manager having the specified number of buffer slots. This
	 * constructor depends on a {@link FileMgr} and {@link simpledb.log.LogMgr
	 * LogMgr} object.
	 * 
	 * @param numbuffs the number of buffer slots to allocate
	 */
	public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
		unpinnedBuffers = new LinkedList<Buffer>();
		allocatedBuffers = new HashMap<BlockId, Buffer>();

		numAvailable = numbuffs;
		for (int i = 0; i < numbuffs; i++)
			unpinnedBuffers.add(new Buffer(fm, lm, i));
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	public synchronized int available() {
		return numAvailable;
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txnum the transaction's id number
	 */
	public synchronized void flushAll(int txnum) {
		for (Buffer buff : unpinnedBuffers)
			if (buff.modifyingTx() == txnum)
				buff.flush();
	}

	/**
	 * Unpins the specified data buffer. If its pin count goes to zero, then notify
	 * any waiting threads.
	 * 
	 * @param buff the buffer to be unpinned
	 */
	public synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned()) {
			numAvailable++;
			unpinnedBuffers.add(buff);
			notifyAll();
		}
	}

	/**
	 * Pins a buffer to the specified block, potentially waiting until a buffer
	 * becomes available. If no buffer becomes available within a fixed time period,
	 * then a {@link BufferAbortException} is thrown.
	 * 
	 * @param blk a reference to a disk block
	 * @return the buffer pinned to that block
	 */
	public synchronized Buffer pin(BlockId blk) {
		try {
			long timestamp = System.currentTimeMillis();
			Buffer buff = tryToPin(blk);
			while (buff == null && !waitingTooLong(timestamp)) {
				wait(MAX_TIME);
				buff = tryToPin(blk);
			}
			if (buff == null)
				throw new BufferAbortException();
			return buff;
		} catch (InterruptedException e) {
			throw new BufferAbortException();
		}
	}

	public void printStatus() {
		StringBuilder sb = new StringBuilder();

		sb.append("Allocated buffers:\n");
		for (Entry<BlockId, Buffer> entry: allocatedBuffers.entrySet()) {
			sb.append("Buffer ");
			sb.append(entry.getValue().getId());
			sb.append(": ");
			sb.append(entry.getKey());
			if (entry.getValue().isPinned()) {
				sb.append(" pinned\n");
			} else {
				sb.append(" unpinned\n");
			}
		}
		
		sb.append("Unpinned Buffers in LRU order:");
		for (Buffer buffer: unpinnedBuffers) {
			sb.append(" ");
			sb.append(buffer.getId());
		}
		
		System.out.println(sb.toString());
	}

	private boolean waitingTooLong(long starttime) {
		return System.currentTimeMillis() - starttime > MAX_TIME;
	}

	/**
	 * Tries to pin a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk a reference to a disk block
	 * @return the pinned buffer
	 */
	private Buffer tryToPin(BlockId blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			BlockId oldBlock = buff.block();
			if (oldBlock != null) {
				allocatedBuffers.remove(buff.block());
			}
			buff.assignToBlock(blk);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin();
		//unpinnedBuffers.remove(buff);
		allocatedBuffers.put(blk, buff);
		return buff;
	}

	private Buffer findExistingBuffer(BlockId blk) {
		return allocatedBuffers.get(blk);
	}

	private Buffer chooseUnpinnedBuffer() {
		if (numAvailable > 0) {
			return unpinnedBuffers.remove(0);
		}
		return null;
	}
}
