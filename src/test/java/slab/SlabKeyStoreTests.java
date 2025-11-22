package slab;

import offHeapMutableAsciiString.UnsafeAsciiString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SlabKeyStoreTests {

    @Test
    public void basicSlabKeyStoreOperations() {
        final ConcreteTestOrder concreteTestOrder = new ConcreteTestOrder();
        final Slab<ConcreteTestOrder> slab = new Slab<>((short) 64, 4, () -> new ConcreteTestOrder());

        final SlabKeyStore<ConcreteTestOrder> slabKeyStore = new SlabKeyStore<>(2048, 0.65f, slab);

        final int index = slab.create(concreteTestOrder);

        concreteTestOrder.getUnsafeAsciiString().set("ABC123");
        slabKeyStore.insert(index, concreteTestOrder);

        slabKeyStore.remove(index, concreteTestOrder);
        slab.removeAt(index);

    }

    @Test
    public void loopingCreateRemoveTest() {
        final TestOrder testOrder = new TestOrder();
        final Slab<TestOrder> slab = new Slab<>((short) 64, 4, () -> new TestOrder());

        final SlabKeyStore<TestOrder> slabKeyStore = new SlabKeyStore<>(2048, 0.65f, slab);

        for (int i = 0; i < 250000000; i++) {
            final int index = slab.create(testOrder);
            testOrder.getUnsafeAsciiString().set(String.valueOf(i));
            slabKeyStore.insert(index, testOrder);
            final int removedKey = slabKeyStore.removeCodec(testOrder);
            slab.removeAt(removedKey);
        }
    }

    @Test
    public void insertionTesting() {
        final TestOrder testOrder = new TestOrder();
        final TestOrder testOrder2 = new TestOrder();
        final UnsafeAsciiString unsafeAsciiString = new UnsafeAsciiString(TestOrder.ASCII_LENGTH);
        final Slab<TestOrder> slab = new Slab<>((short) 64, 4, () -> new TestOrder());
        final SlabKeyStore<TestOrder> slabKeyStore = new SlabKeyStore<>(8, 0.65f, slab);

        for (int i = 0; i < 10; i++) {
            final int index = slab.create(testOrder);
            testOrder.getUnsafeAsciiString().set(String.valueOf(i));
            slabKeyStore.insert(index, testOrder);
            System.out.println(slabKeyStore);
        }

        assertEquals(10, slabKeyStore.size());
        System.out.println(slabKeyStore);
        for (int i = 0; i < 10; i++) {
            unsafeAsciiString.set(String.valueOf(i));
            final int index = slabKeyStore.wrapFromKey(
                    unsafeAsciiString.buffer(), 0,
                    BufferUtils.segmentHashCodeShortCircuiting(
                            unsafeAsciiString.buffer(), 0, TestOrder.ASCII_LENGTH));
            assertEquals(i, index);
            testOrder2.getUnsafeAsciiString().set(String.valueOf(i));
            final int index2 = slabKeyStore.wrapFromKey(testOrder2);
            assertEquals(i, index2);
        }
        testOrder2.getUnsafeAsciiString().set("MISS");
        final int index = slabKeyStore.wrapFromKey(testOrder2);
        assertEquals(-1, index);

        testOrder2.getUnsafeAsciiString().set("INVALID");
        final int index2 = slabKeyStore.wrapFromKey(testOrder2);
        assertEquals(-1, index2);

        for (int i = 0; i < 10; i++) {
            testOrder2.getUnsafeAsciiString().set(String.valueOf(i));
            final int removedIndex = slabKeyStore.removeCodec(testOrder2);
            assertEquals(i, removedIndex);
            System.out.println(slabKeyStore);
        }
    }
}
