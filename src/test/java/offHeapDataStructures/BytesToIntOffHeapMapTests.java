package offHeapDataStructures;

import offHeapMutableAsciiString.UnsafeAsciiString;
import org.agrona.collections.Object2LongHashMap;
import org.junit.jupiter.api.Test;
import slab.ConcreteTestCodec;
import slab.TestCodec;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BytesToIntOffHeapMapTests {

    @Test
    public void testInsertions() {
        final BytesToIntOffHeapMap<TestCodec> testCodecMap
                = new BytesToIntOffHeapMap<>(TestCodec::new);

        final TestCodec testCodec = new ConcreteTestCodec();
        testCodec.setId(4);
        testCodecMap.put(testCodec, 7);
        assertEquals(1, testCodecMap.size());
        final TestCodec testCodec2 = new ConcreteTestCodec();
        testCodec2.setId(4);
        final int testValue = testCodecMap.getInt(testCodec2);
        assertEquals(7, testValue);
    }


    @Test
    public void loopingCreateRemoveTest() {
        final UnsafeAsciiString reusableCodec = new UnsafeAsciiString(40);
        final UnsafeAsciiString lookupCodec = new UnsafeAsciiString(40);

        final Object2LongHashMap<UnsafeAsciiString> map
                = new Object2LongHashMap<>(2048, 0.65f, -1);

        for (int i = 0; i < 50000000; i++) {
            reusableCodec.set(String.valueOf(i));
            lookupCodec.set(String.valueOf(i));
            map.put(reusableCodec, i);
            map.removeKey(lookupCodec);
        }
        final BytesToIntOffHeapMap<UnsafeAsciiString> offHeapMap
                = new BytesToIntOffHeapMap<>(2048, () -> new UnsafeAsciiString(40));

        for (int i = 0; i < 50000000; i++) {
            reusableCodec.set(String.valueOf(i));
            lookupCodec.set(String.valueOf(i));
            offHeapMap.putValue(reusableCodec, i);
            offHeapMap.removeKey(lookupCodec);
        }
    }
}
