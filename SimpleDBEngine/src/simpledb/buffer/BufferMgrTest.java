package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.*;

public class BufferMgrTest {

    static void mruBasicTest() {

        System.out.println("\n\n---- MRU BASIC TEST ----\n\n");
        SimpleDB db = new SimpleDB("buffermgrtest", 400, 5); // only 3 buffers
        BufferMgr bm = db.bufferMgr();

        Buffer[] buff = new Buffer[5];
        for (int i = 0; i < 5; i++) {
            buff[i] = bm.pin(new BlockId("testfile", i + 1));
        }
        bm.unpin(buff[3]);
        bm.unpin(buff[2]);
        bm.pin(new BlockId("testfile", 21)); // Should go to buff[2]
        bm.pin(new BlockId("testfile", 30)); // Should go to buff[3]

        assert buff[2].block().number() == 21;
        assert buff[3].block().number() == 30;

        for (int i = 0; i < buff.length; i++) {
            Buffer b = buff[i];
            if (b != null)
                System.out.println("buff[" + i + "] pinned to block " + b.block());
        }
        buff[2].setModified(1, -1); // Mark Buffer 2 as modified
        bm.unpin(buff[3]);
        bm.unpin(buff[2]);
        bm.pin(new BlockId("testfile", 22)); // Should go to buff[3]
        bm.pin(new BlockId("testfile", 31)); // Should go to buff[2]

        assert buff[2].block().number() == 31;
        assert buff[3].block().number() == 22;

        System.out.println();
        for (int i = 0; i < buff.length; i++) {
            Buffer b = buff[i];
            if (b != null)
                System.out.println("buff[" + i + "] pinned to block " + b.block());
        }
    }

    static void basicTest() {
        System.out.println("\n\n---- BASIC TEST ----\n\n");
        SimpleDB db = new SimpleDB("buffermgrtest", 400, 3); // only 3 buffers
        BufferMgr bm = db.bufferMgr();
        Buffer[] buff = new Buffer[6];
        buff[0] = bm.pin(new BlockId("testfile", 0));
        buff[1] = bm.pin(new BlockId("testfile", 1));
        buff[2] = bm.pin(new BlockId("testfile", 2));
        bm.unpin(buff[1]);
        buff[1] = null;
        buff[3] = bm.pin(new BlockId("testfile", 0)); // block 0 pinned twice
        buff[4] = bm.pin(new BlockId("testfile", 1)); // block 1 repinned
        System.out.println("Available buffers: " + bm.available());
        try {
            System.out.println("Attempting to pin block 3...");
            buff[5] = bm.pin(new BlockId("testfile", 3)); // will not work; no buffers left
        } catch (BufferAbortException e) {
            System.out.println("Exception: No available buffers\n");
        }
        bm.unpin(buff[2]);
        buff[2] = null;
        buff[5] = bm.pin(new BlockId("testfile", 3)); // now this works

        System.out.println("Final Buffer Allocation:");
        for (int i = 0; i < buff.length; i++) {
            Buffer b = buff[i];
            if (b != null)
                System.out.println("buff[" + i + "] pinned to block " + b.block());
        }
    }

    public static void main(String[] args) throws Exception {
        basicTest();
        mruBasicTest();
    }
}
