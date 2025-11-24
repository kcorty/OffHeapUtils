package slab;

import offHeapMutableAsciiString.UnsafeAsciiString;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class TestOrder implements Codec {

    protected final UnsafeAsciiString unsafeAsciiString = new UnsafeAsciiString();

    private final MutableDirectBuffer buffer = new UnsafeBuffer();

    public static final short ASCII_OFFSET = 0;
    public static final short ASCII_LENGTH = 40;

    @Override
    public short bufferSize() {
        return 256;
    }

    @Override
    public void wrap(final MutableDirectBuffer buffer, final int offset, final int length) {
        this.buffer.wrap(buffer, offset, length);
    }

    @Override
    public DirectBuffer buffer() {
        return buffer;
    }

    @Override
    public int keyHashCode() {
        return BufferUtils.segmentHashCodeShortCircuiting(this.buffer, ASCII_OFFSET, ASCII_LENGTH);
    }

    @Override
    public int generateKeyHashCode(final MutableDirectBuffer buffer, final int offset) {
        return BufferUtils.segmentHashCodeShortCircuiting(buffer, offset + ASCII_OFFSET, ASCII_LENGTH);
    }

    @Override
    public int keyOffset() {
        return ASCII_OFFSET;
    }

    @Override
    public int keyLength() {
        return ASCII_LENGTH;
    }

    public UnsafeAsciiString getUnsafeAsciiString() {
        unsafeAsciiString.wrap(this.buffer, ASCII_OFFSET, ASCII_LENGTH);
        return unsafeAsciiString;
    }

}
