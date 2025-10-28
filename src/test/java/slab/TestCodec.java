package slab;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class TestCodec implements Codec {
    protected static final int BUFFER_SIZE = 4;

    private final MutableDirectBuffer buffer = new UnsafeBuffer();

    @Override
    public short bufferSize() {
        return BUFFER_SIZE;
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
        return buffer.hashCode();
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    public int getId() {
        return this.buffer.getInt(0);
    }

    public void setId(final int id) {
        this.buffer.putInt(0, id);
    }
}
