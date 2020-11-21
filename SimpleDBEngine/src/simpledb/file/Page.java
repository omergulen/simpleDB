package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.*;

public class Page {
	private ByteBuffer bb;
	public static Charset CHARSET = StandardCharsets.US_ASCII;

	// For creating data buffers
	public Page(int blocksize) {
		bb = ByteBuffer.allocateDirect(blocksize);
	}

	// For creating log pages
	public Page(byte[] b) {
		bb = ByteBuffer.wrap(b);
	}

	// TODO :: Handle if offset can't have an integer.
	public int getInt(int offset) {
		return bb.getInt(offset);
	}

	public void setInt(int offset, int n) {
		if (bb.capacity() - 4 < offset) {
			// Integer is 32 bit = 4 bytes
			System.out.println("The integer " + n + " does not fit at location " + offset + " of the page.");
		} else {
			bb.putInt(offset, n);
		}
	}

	// TODO :: Handle if offset can't have an bytes.
	public byte[] getBytes(int offset) {
		bb.position(offset);
		int length = bb.getInt();
		byte[] b = new byte[length];
		bb.get(b);
		return b;
	}

	public void setBytes(int offset, byte[] b) {
		int maxLength = b.length + 4; // Integer is 32 bit = 4 bytes
		if (bb.capacity() - maxLength < offset) {
			String bytesStr = new String(b, StandardCharsets.UTF_8);
			System.out.println("Bytes " + bytesStr + " does not fit at location " + offset + " of the page.");
		} else {
			bb.position(offset);
			bb.putInt(b.length);
			bb.put(b);
		}
	}

	public String getString(int offset) {
		byte[] b = getBytes(offset);
		return new String(b, CHARSET);
	}

	public void setString(int offset, String s) {
		byte[] b = s.getBytes(CHARSET);
		setBytes(offset, b);
	}

	public static int maxLength(int strlen) {
		float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
		return Integer.BYTES + (strlen * (int) bytesPerChar);
	}

	// a package private method, needed by FileMgr
	ByteBuffer contents() {
		bb.position(0);
		return bb;
	}
}
