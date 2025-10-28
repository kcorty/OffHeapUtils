package slab;

import org.junit.jupiter.api.Test;

public class SlabKeyStoreTests {

    @Test
    public void basicSlabKeyStoreOperations() {
        final ConcreteTestOrder concreteTestOrder = new ConcreteTestOrder();
        final Slab<ConcreteTestOrder> slab = new Slab<>((short) 64, 4, () -> new ConcreteTestOrder());

        final SlabKeyStore<ConcreteTestOrder> slabKeyStore = new SlabKeyStore<>(2048, 0.65f,
                ConcreteTestOrder.ASCII_OFFSET, ConcreteTestOrder.ASCII_LENGTH, slab);

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

        final SlabKeyStore<TestOrder> slabKeyStore = new SlabKeyStore<>(2048, 0.65f,
                TestOrder.ASCII_OFFSET, TestOrder.ASCII_LENGTH, slab);

        for (int i = 0; i < 250000000; i++) {
            final int index = slab.create(testOrder);
            testOrder.getUnsafeAsciiString().set(String.valueOf(i));
            slabKeyStore.insert(index, testOrder);
            slabKeyStore.removeAt(index);
            slab.removeAt(index);
        }
    }
}
