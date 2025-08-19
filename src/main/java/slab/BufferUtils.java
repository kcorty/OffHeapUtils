package slab;

import org.agrona.MutableDirectBuffer;

public class BufferUtils {

    public static void resetBuffer(final MutableDirectBuffer buffer) {
        buffer.setMemory(0, buffer.capacity(), (byte) 0);
    }

    public static void resetBuffer(final MutableDirectBuffer buffer, final int offset) {
        buffer.setMemory(offset, buffer.capacity() - offset, (byte) 0);
    }

    public static void resetBuffer(final MutableDirectBuffer buffer, final int offset, final int length) {
        buffer.setMemory(offset, length, (byte) 0);
    }
}
