package slab;

import offHeapTypes.DirectBufferUnsafeString;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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

        slabKeyStore.removeCodec(concreteTestOrder);
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
    public void loopingCreateThenRemoveTest() {
        final TestOrder testOrder = new TestOrder();
        final TestOrder testOrder2 = new TestOrder();
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(40)
                .order(ByteOrder.LITTLE_ENDIAN));
        testOrder2.wrap(unsafeBuffer, 0, 40);
        final Slab<TestOrder> slab = new Slab<>((short) 64, 2 << 15, () -> new TestOrder());

        final SlabKeyStore<TestOrder> slabKeyStore = new SlabKeyStore<>(2 << 22, 0.65f, slab);

        for (int i = 0; i < 2000000; i++) {
            final int index = slab.create(testOrder);
            testOrder.getUnsafeAsciiString().set(String.valueOf(i));
            slabKeyStore.insert(index, testOrder);
        }

        for (int i = 0; i < 2000000; i++) {
            testOrder2.getUnsafeAsciiString().set(String.valueOf(i));
            final int removedKey = slabKeyStore.removeCodec(testOrder2);
            assertEquals(removedKey, i);
            slab.removeAt(removedKey);
        }
    }

    @Test
    public void heapLoopingCreateThenRemoveTest() {
        final Object2ObjectHashMap<DirectBufferUnsafeString, ConcreteTestOrder> heapMap = new Object2ObjectHashMap<>(
                2 << 22, 0.65f);

        for (int i = 0; i < 2000000; i++) {
            final ConcreteTestOrder concreteTestOrder = new ConcreteTestOrder();
            concreteTestOrder.getUnsafeAsciiString().set(String.valueOf(i));
            heapMap.put(concreteTestOrder.getUnsafeAsciiString(), concreteTestOrder);
        }

        for (int i = 0; i < 2000000; i++) {
            final ConcreteTestOrder concreteTestOrder = new ConcreteTestOrder();
            concreteTestOrder.getUnsafeAsciiString().set(String.valueOf(i));
            heapMap.remove(concreteTestOrder.getUnsafeAsciiString());
        }
    }

    @Test
    public void insertionTesting() {
        final TestOrder testOrder = new TestOrder();
        final TestOrder testOrder2 = new TestOrder();
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(40)
                .order(ByteOrder.LITTLE_ENDIAN));
        testOrder2.wrap(unsafeBuffer, 0, 40);
        final DirectBufferUnsafeString directBufferUnsafeString = new DirectBufferUnsafeString(TestOrder.ASCII_LENGTH);
        final Slab<TestOrder> slab = new Slab<>((short) 64, 4, () -> new TestOrder());
        final SlabKeyStore<TestOrder> slabKeyStore = new SlabKeyStore<>(8, 0.65f, slab);

        for (int i = 0; i < 10; i++) {
            final int index = slab.create(testOrder);
            testOrder.getUnsafeAsciiString().set(String.valueOf(i));
            slabKeyStore.insert(index, testOrder);
            System.out.println(slabKeyStore.printDataStore());
        }

        assertEquals(10, slabKeyStore.size());
        System.out.println(slabKeyStore.printDataStore());
        for (int i = 0; i < 10; i++) {
            testOrder2.getUnsafeAsciiString().set(String.valueOf(i));
            final int index2 = slabKeyStore.getKey(testOrder2);
            assertEquals(i, index2);
        }
        testOrder2.getUnsafeAsciiString().set("MISS");
        final int index = slabKeyStore.getKey(testOrder2);
        assertEquals(-1, index);

        testOrder2.getUnsafeAsciiString().set("INVALID");
        final int index2 = slabKeyStore.getKey(testOrder2);
        assertEquals(-1, index2);

        for (int i = 0; i < 5; i++) {
            testOrder2.getUnsafeAsciiString().set(String.valueOf(i));
            final int removedIndex = slabKeyStore.removeCodec(testOrder2);
            assertEquals(i, removedIndex);
            System.out.println(slabKeyStore.printDataStore());
        }
    }
}
