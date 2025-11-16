package slab;

import org.agrona.DirectBuffer;
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

    public static boolean bufferEquals(final DirectBuffer buffer, final DirectBuffer other,
                                       final int otherOffset, final int otherLength) {
        int i = 0;

        for (final int end = otherLength & ~7; i < end; i +=8) {
            if (buffer.getLong(i) != other.getLong(otherOffset + i)) {
                return false;
            }
        }
        for (; i < otherLength; i++) {
            if (buffer.getByte(i) != other.getByte(otherOffset + i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean bufferEquals(final DirectBuffer buffer, final int bufferOffset,
                                       final DirectBuffer other, final int otherOffset, final int otherLength) {
        int i = 0;

        for (final int end = otherLength & ~7; i < end; i +=8) {
            if (buffer.getLong(bufferOffset + i) != other.getLong(otherOffset + i)) {
                return false;
            }
        }
        for (; i < otherLength; i++) {
            if (buffer.getByte(bufferOffset + i) != other.getByte(otherOffset + i)) {
                return false;
            }
        }
        return true;
    }

    public static int segmentHashCode(final DirectBuffer buffer, final int offset,
                                      final int length) {
        int i = 0, hashCode = 19;
        for (final int end = length & ~7; i < end; i += 8) {
            hashCode = hashCode * 31 + Long.hashCode(buffer.getLong(offset + i));
        }

        for (; i < length; i++) {
            hashCode = hashCode * 31 + buffer.getByte(offset + i);
        }
        return hashCode;
    }

    public static int segmentHashCodeShortCircuiting(final DirectBuffer buffer, final int offset,
                                                     final int length) {
        int i = 0, hashCode = 19;
        for (final int end = length & ~7; i < end; i += 8) {
            final long value = buffer.getLong(offset + i);
            if (value == 0) {
                return hashCode;
            }
            hashCode = hashCode * 31 + Long.hashCode(value);
        }

        for (; i < length; i++) {
            hashCode = hashCode * 31 + buffer.getByte(offset + i);
        }
        return hashCode;
    }
}
