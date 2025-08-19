package slab;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SlabPageTests {

    @Test
    public void slabPageTest() {
        final TestCodec testCodec = new TestCodec();
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(16));
        final SlabPage<TestCodec> slabPage = new SlabPage<>(buffer, testCodec.bufferSize(), 1);

        assertEquals(1, slabPage.getPageIndex());

        assertEquals(0, slabPage.getLiveCount());
        slabPage.createAt(0, testCodec);
        assertEquals(1, slabPage.getLiveCount());
        assertEquals(0, buffer.getInt(9));
        testCodec.setId(3);
        assertEquals(3, buffer.getInt(9));

        slabPage.createAt(1, testCodec);

        assertEquals(2, slabPage.getLiveCount());
        testCodec.setId(4);
        assertEquals(4, buffer.getInt(14));

        slabPage.removeAt(0);
        assertEquals(1, slabPage.getLiveCount());
        assertEquals(4, testCodec.getId());

        slabPage.cleanPage();
        assertEquals(0, slabPage.getLiveCount());
        assertEquals(0, slabPage.getPageIndex());
        assertEquals(0, testCodec.getId());
    }
}
