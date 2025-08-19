package offHeapMutableAsciiString;

import org.agrona.collections.Object2LongHashMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class MemorySegmentAsciiStringTest {

    @Test
    public void testMemorySegmentAsciiString() {
        final MemorySegmentAsciiString segment1 = new MemorySegmentAsciiString(40);
        final MemorySegmentAsciiString segment2 = new MemorySegmentAsciiString(40);
        final MemorySegmentAsciiString segment3 = new MemorySegmentAsciiString(48);
        final MemorySegmentAsciiString segment4 = new MemorySegmentAsciiString(48);

        segment1.set("ABCD1234567890");
        segment2.set("ABCD1234567890");
        segment3.set("ABCD1234567890");
        segment4.set("ABCD123456789");

        assertEquals(segment1, segment2);
        assertEquals(segment1, segment3);

        assertNotEquals(segment3, segment4);

    }

    @Test
    public void mapTest() {
        final Object2LongHashMap<MemorySegmentAsciiString> map = new Object2LongHashMap<>(1000000, 0.8f, -1);

        for (int i = 0; i < 500000; i++) {
            final MemorySegmentAsciiString segment1 = new MemorySegmentAsciiString(40);
            segment1.set(String.valueOf(i));
            map.put(segment1, i);
        }

        final MemorySegmentAsciiString reuseable = new MemorySegmentAsciiString(40);

        for (int i = 0; i < 500000; i++) {
            reuseable.set(String.valueOf(i));
            final long index = map.getValue(reuseable);
            assertEquals(index, i);
        }
    }
}
