package offHeapMutableAsciiString;

import lombok.Getter;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import slab.Codec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.InputMismatchException;

public class UnsafeAsciiString implements CharSequence, Codec {
    @Getter
    private final MutableDirectBuffer buffer;

    public UnsafeAsciiString() {
        this.buffer = new UnsafeBuffer();
    }

    public UnsafeAsciiString(final int size) {
        if ((size & 7) != 0) {
            throw new InputMismatchException("Buffer size must be word aligned to 8 bytes!");
        }
        this.buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN));
    }

    public UnsafeAsciiString(final DirectBuffer otherBuffer, final int offset, final int size) {
        if ((size & 7) != 0) {
            throw new InputMismatchException("Buffer size must be word aligned to 8 bytes!");
        }
        this.buffer = new UnsafeBuffer(otherBuffer, offset, size);
    }

    public void wrap(final DirectBuffer buffer, final int offset, final int length) {
        this.buffer.wrap(buffer, offset, length);
    }

    public void set(final CharSequence charSequence) {
        if (charSequence.length() > buffer.capacity()) {
            throw new IndexOutOfBoundsException("CharSequence too long!");
        }
        int i = 0;
        for (; i < charSequence.length(); i++) {
            this.buffer.putChar(i, charSequence.charAt(i));
        }
        if (charSequence.length() == buffer.capacity()) {
            return;
        }
        padRemainder(charSequence.length());
    }

    public void set(final UnsafeAsciiString otherBuffer) {
        if (otherBuffer.buffer.capacity() > this.buffer.capacity()) {
            throw new IndexOutOfBoundsException("Buffer too long!");
        }
        this.buffer.putBytes(0, otherBuffer.buffer, 0, otherBuffer.buffer.capacity());
        if (otherBuffer.buffer.capacity() == buffer.capacity()) {
            return;
        }
        padRemainder(otherBuffer.buffer.capacity());
    }

    public void encodeTo(final MutableDirectBuffer dstBuffer, final int offset) {
        dstBuffer.putBytes(offset, this.buffer, 0, this.buffer.capacity());
    }

    public void reset() {
        padRemainder(0);
    }

    @Override
    public UnsafeAsciiString clone() {
        final UnsafeAsciiString clone = new UnsafeAsciiString(this.buffer.capacity());
        clone.set(this);
        return clone;
    }

    @Override
    public int hashCode() {
        int hashCode = 19;
        for (int i = 0; i + 8 <= this.buffer.capacity(); i += 8) {
            final var value = this.buffer.getLong(i);
            if (value == 0) {
                return hashCode;
            }
            hashCode = hashCode * 31 + Long.hashCode(value);
        }
        return hashCode;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object instanceof final UnsafeAsciiString other) {
            if (this.buffer.capacity() == other.buffer.capacity()) {
                return this.buffer.compareTo(other.buffer) == 0;
            }
            if (this.buffer.capacity() > other.buffer.capacity()) {
                return unsafeEquals(other, this);
            }
            return unsafeEquals(this, other);
        }

        if (object instanceof final CharSequence otherCharSequence) {
            return CharSequenceUtils.equals(this, otherCharSequence);
        }
        return false;
    }

    //TODO: Explore whether backwards heuristic is quicker
    public static boolean unsafeEquals(final UnsafeAsciiString a, final UnsafeAsciiString b) {
        for (int i = 0; i + 8 <= a.buffer.capacity(); i += 8) {
            if (a.buffer.getLong(i) != b.buffer.getLong(i)) {
                return false;
            }
        }
        return true;
    }

    //TODO: Explore whether backwards heuristic is quicker
    public void padRemainder(final int beginOffset) {
        final int length = this.buffer.capacity();
        int i = beginOffset;
        for (; i + 8 <= length; i += 8) {
            buffer.putLong(i, 0);
        }

        if (length - i >= 4) {
            buffer.putInt(i, 0);
        }
        while (i < length) {
            buffer.putByte(i++, (byte) 0);
        }
    }

    @Override
    public int length() {
        return getLength();
    }

    @Override
    public char charAt(int index) {
        if (index < 0 || index >= length()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + length());
        }
        return this.buffer.getChar(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        if (start < 0 || end < 0) {
            throw new IndexOutOfBoundsException("Index: " + start + ", Length: " + end);
        }
        final var length = getLength();
        if (start > length || end > length) {
            throw new IndexOutOfBoundsException("Index: " + start + ", Length: " + length);
        }
        return this.buffer.getStringAscii(start, end);
    }

    @Override
    public String toString() {
        return this.buffer.getStringAscii(0);
    }

    //TODO: parallelize on long and then recur when long == 0
    public int getLength() {
        int length = 0;
        while (length < this.buffer.capacity()) {
            final byte b = buffer.getByte(length);
            if (b == 0) {
                return length;
            }
            length++;
        }
        return length;
    }

    @Override
    public short bufferSize() {
        return (short) this.buffer.capacity();
    }

    @Override
    public void wrap(final MutableDirectBuffer buffer, final int offset, final int length) {
        this.buffer.wrap(buffer, offset, length);
    }

    @Override
    public DirectBuffer buffer() {
        return this.buffer;
    }

    @Override
    public int keyHashCode() {
        return hashCode();
    }
}
