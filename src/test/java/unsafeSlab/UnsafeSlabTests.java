package unsafeSlab;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnsafeSlabTests {
    @Test
    public void slabAddsPageWhenCleanExhausted() {
        final UnsafeSlab<UnsafeTestOrder> slab = new UnsafeSlab<>((short) 64, 4, UnsafeTestOrder::new);

        final UnsafeTestOrder testCodec = new UnsafeTestOrder();
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
        final UnsafeSlab<UnsafeTestOrder> slab = new UnsafeSlab<>((short) 256, 8192, UnsafeTestOrder::new);

        final UnsafeTestOrder testCodec = new UnsafeTestOrder();
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
        final UnsafeSlab<UnsafeTestOrder> slab = new UnsafeSlab<>((short) 256, 4096, () -> new UnsafeTestOrder());

        final UnsafeTestOrder testCodec = new UnsafeTestOrder();
        final int iterations = 256 * 4096 * 16;
        for (int i = 0; i < iterations; i++) {
            final int index = slab.create(testCodec);
            slab.removeAt(index);
        }
    }
}
