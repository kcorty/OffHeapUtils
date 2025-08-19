package slab;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SlabTests {

    @Test
    public void slabAddsPageWhenCleanExhausted() {
        final Slab<TestCodec> slab = new Slab<>((short) 64, 4, () -> new TestCodec());

        final TestCodec testCodec = new TestCodec();
        for (int i = 0; i < 320; i++) {
            assertEquals(i, slab.create(testCodec));
        }

        for (int i = 0; i < 63; i++) {
            slab.removeAt(i);
        }
        for (int i = 320; i < 383; i++) {
            assertEquals(i, slab.create(testCodec));
        }
        slab.removeAt(63);
        assertEquals(383, slab.create(testCodec));
        for (int i = 0; i < 64; i++) {
            assertEquals(i, slab.create(testCodec));
        }
    }

    @Test
    public void slabIterationTest() {
        final Slab<TestOrder> slab = new Slab<>((short) 256, 8192, () -> new TestOrder());

        final TestOrder testCodec = new TestOrder();
        final int iterations = 256 * 8192;
        for (int i = 0; i < iterations; i++) {
            assertEquals(i, slab.create(testCodec));
        }

        for (int i = 0; i < iterations; i++) {
            slab.getAt(i, testCodec);
        }

        for (int i = 0; i < iterations; i++) {
            slab.removeAt(i);
        }
    }

    @Test
    public void slabRemoveTest() {
        final Slab<TestOrder> slab = new Slab<>((short) 256, 4096, () -> new TestOrder());

        final TestOrder testCodec = new TestOrder();
        final int iterations = 256 * 4096 * 16;
        for (int i = 0; i < iterations; i++) {
            final int index = slab.create(testCodec);
            slab.removeAt(index);
        }
    }

}
