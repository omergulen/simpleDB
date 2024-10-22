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

		// First print blocks' allocated buffers
		// Yet, there can be missing buffer since we keep blockIds as keys
		// If the same block is allocated to another buffer, even if the old
		// buffer keeps the block, the <BlockId, Buffer> map won't know about the old buffer
		// To prevent duplications on the status printing, printing only the pinned buffers.
		sb.append("Allocated buffers:\n");
		for (Entry<BlockId, Buffer> entry: allocatedBuffers.entrySet()) {
			Buffer tmpBuffer = entry.getValue();
			if (tmpBuffer.isPinned()) {
				BlockId tmpBlock = entry.getKey();
				sb.append("Buffer ");
				sb.append(tmpBuffer.getId());
				sb.append(": ");
				sb.append(tmpBlock);
				sb.append(" pinned\n");
			}
		}
		
		// Then, iterate through the unpinnedBuffers to print the buffers
		// which are unpinned, in the end we will be printing all buffers' statuses.
		StringBuilder sb2 = new StringBuilder();
		sb2.append("Unpinned Buffers in LRU order:");
		for (Buffer buffer: unpinnedBuffers) {
			sb.append("Buffer ");
			sb.append(buffer.getId());
			sb.append(": ");
			sb.append(buffer.block());
			sb.append(" unpinned\n");
			
			sb2.append(" ");
			sb2.append(buffer.getId());
		}

		sb.append(sb2);
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
		allocatedBuffers.put(blk, buff);
		return buff;
	}

	private Buffer findExistingBuffer(BlockId blk) {
		Buffer buff = allocatedBuffers.get(blk);
		if (buff != null && buff.isPinned()) {
			return buff;
		}
		
		return null;
	}

	private Buffer chooseUnpinnedBuffer() {
		if (numAvailable > 0) {
			return unpinnedBuffers.remove(0);
		}
		return null;
	}
}
