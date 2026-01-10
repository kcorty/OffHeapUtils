package offHeapTypes;

import org.agrona.UnsafeApi;
import unsafeSlab.UnsafeCodec;
import utils.UnsafeBufferUtils;

import java.nio.charset.StandardCharsets;

import static org.agrona.BufferUtil.ARRAY_BASE_OFFSET;

public class UnsafeString implements UnsafeCodec, CharSequence {
    private long memOffset;
    private final int length;

    public UnsafeString(final int length) {
        this.memOffset = UnsafeApi.allocateMemory(length);
        this.length = length;
    }

    public UnsafeString(final long memOffset, final int length) {
        this.memOffset = memOffset;
        this.length = length;
    }

    public void wrap(final long memOffset) {
        this.memOffset = memOffset;
    }

    public void set(final CharSequence charSequence) {
        if (charSequence.length() > length) {
            throw new IndexOutOfBoundsException("CharSequence too long!");
        }

        int i = 0;
        for (; i < charSequence.length(); i++) {
            UnsafeApi.putChar(memOffset + i, charSequence.charAt(i));
        }

        padRemainder(charSequence.length());
    }

    public void set(final UnsafeString other) {
        if (other.length > length) {
            throw new IndexOutOfBoundsException("Length too long!");
        }

        UnsafeApi.copyMemory(other.memOffset, memOffset, length);
        if (other.length == length) {
            return;
        }
        padRemainder(other.length);
    }

    public void encodeTo(final long targetMemOffset, final int targetLength) {
        UnsafeApi.copyMemory(memOffset, targetMemOffset, length);
    }

    public void reset() {
        padRemainder(0);
    }

    @Override
    public UnsafeString clone() {
        final UnsafeString clone = new UnsafeString(length);
        clone.set(this);
        return clone;
    }

    @Override
    public int hashCode() {
        return UnsafeBufferUtils.unsafeHashCodeShortCircuiting(memOffset, length);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        return switch (o) {
            case final UnsafeString other -> UnsafeBufferUtils.bufferEquals(memOffset, other.memOffset, length);
            case final CharSequence other -> CharSequenceUtils.equals(this, other);
            default -> false;
        };
    }


    public int capacity() {
        return length;
    }

    public void padRemainder(final int beginOffset) {
        int i = beginOffset;
        for (final int end = length & ~7; i < end; i += 8) {
            UnsafeApi.putLong(memOffset + i, 0);
        }
        for (; i < length; i++) {
            UnsafeApi.putByte(memOffset + i, (byte) 0);
        }
    }

    @Override
    public int length() {
        return getLength();
    }

    @Override
    public char charAt(final int index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("Index out of bounds!");
        }
        return UnsafeApi.getChar(memOffset + index);
    }

    @Override
    public CharSequence subSequence(final int start, final int end) {
        if (start < 0 || end > length) {
            throw new IndexOutOfBoundsException("Index out of bounds!");
        }
        final var length = getLength();
        if (start > length || end < length) {
            throw new IndexOutOfBoundsException("Index out of bounds!");
        }
        final byte[] dst = new byte[length];
        UnsafeApi.copyMemory(null, memOffset + start, dst, ARRAY_BASE_OFFSET, end - start);
        return new String(dst, StandardCharsets.US_ASCII);
    }

    @Override
    public String toString() {
        return subSequence(0, length).toString();
    }


    public int getLength() {
        int length = 0;
        while (length < this.length) {
            final byte b = UnsafeApi.getByte(memOffset + length);
            if (b == 0) {
                return length;
            }
            length++;
        }
        return length;
    }

    @Override
    public short bufferSize() {
        return (short) length;
    }

    @Override
    public long memOffset() {
        return memOffset;
    }

    @Override
    public int generateKeyHashCode(long codecOffset) {
        return hashCode();
    }

    @Override
    public int keyOffset() {
        return 0;
    }

    @Override
    public int keyLength() {
        return length;
    }
}
