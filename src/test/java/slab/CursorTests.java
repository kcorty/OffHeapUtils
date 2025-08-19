package slab;

import org.agrona.collections.IntArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CursorTests {

    private int activePagesCount = 0;
    private final SlabPage<TestCodec>[] pages = new SlabPage[4];
    private final IntArrayQueue cleanPageIndices = new IntArrayQueue();
    private UnsafeBuffer buffer;

    private int pageCount = 0;

    @Test
    public void cursorTests() {
        addPage();
        final Cursor<TestCodec> cursor = new Cursor<>(2, cleanPageIndices, this::addPage, pages);
        final TestCodec testCodec = new TestCodec();

        assertEquals(0, cursor.getCursorIndex());
        cursor.wrapAtCursor(testCodec);
        assertEquals(0, this.buffer.getInt(9));
        testCodec.setId(3);
        assertEquals(3, this.buffer.getInt(9));
        cursor.incrementCursor();
        assertEquals(1, cursor.getCursorIndex());
        cursor.wrapAtCursor(testCodec);
        assertEquals(0, testCodec.getId());
        assertEquals(0, this.buffer.getInt(14));

        //will exceed existing pages, triggering new page
        cursor.incrementCursor();
        assertEquals(2, cursor.getCursorIndex());
        assertEquals(0, this.buffer.getInt(9));
        cursor.wrapAtCursor(testCodec);
        assertEquals(0, testCodec.getId());
        testCodec.setId(4);
        assertEquals(4, this.buffer.getInt(9));
    }

    private void addPage() {
        this.buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(18));
        final SlabPage<TestCodec> slabPage = new SlabPage<>(buffer, (short) 4, pageCount++);
        pages[activePagesCount] = slabPage;
        cleanPageIndices.addInt(activePagesCount++);
    }
}
