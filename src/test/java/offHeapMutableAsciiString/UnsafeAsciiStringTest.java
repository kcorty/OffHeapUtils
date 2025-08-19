package offHeapMutableAsciiString;

import org.agrona.collections.Object2LongHashMap;
import org.junit.jupiter.api.Test;

import java.util.InputMismatchException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnsafeAsciiStringTest {

    @Test
    public void testUnsafeEquals() throws InputMismatchException {
        assertEquals(new UnsafeAsciiString(40), new  UnsafeAsciiString(40));
    }

    @Test
    public void mapTest() {
        final Object2LongHashMap<UnsafeAsciiString> map = new Object2LongHashMap<>(1000000, 0.8f, -1);

        for (int i = 0; i < 500000; i++) {
            final UnsafeAsciiString unsafeAsciiString = new UnsafeAsciiString(40);
            unsafeAsciiString.set(String.valueOf(i));
            map.put(unsafeAsciiString, i);
        }

        final UnsafeAsciiString reusable = new UnsafeAsciiString(40);

        for (int i = 0; i < 500000; i++) {
            reusable.set(String.valueOf(i));
            final long index = map.getValue(reusable);
            assertEquals(i, index);
        }
    }


}
