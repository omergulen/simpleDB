package simpledb.file;

import java.io.*;
import java.nio.charset.StandardCharsets;

import simpledb.server.SimpleDB;

public class FileTest {
   public static void main(String[] args) throws IOException {
      SimpleDB db = new SimpleDB("filetest", 400, 8);
      FileMgr fm = db.fileMgr();
      BlockId blk = new BlockId("testfile", 2);
      int pos1 = 300;

      Page p1 = new Page(fm.blockSize());
      p1.setString(pos1, "abcdefgxxxxhijklm");
      int size = Page.maxLength("abcdefgxxxxhijklm".length());
      int pos2 = pos1 + size;
      p1.setInt(pos2, 345);
      fm.write(blk, p1);

      Page p2 = new Page(fm.blockSize());
      fm.read(blk, p2);
      byte[] b = "a".getBytes();
      int pos = 396;
      p2.setBytes(pos, b);

      // String str = new String(p2.getBytes(pos), StandardCharsets.UTF_8);
      System.out.println("offset " + pos2 + " contains " + p2.getInt(pos2));
      System.out.println("offset " + pos1 + " contains " + p2.getString(pos1));
      // System.out.println("offset " + pos + " contains " + str);
   }
}