package slab;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class TestCodec implements Codec {

    private final MutableDirectBuffer buffer = new UnsafeBuffer();

    @Override
    public short bufferSize() {
        return 4;
    }

    @Override
    public void wrap(final MutableDirectBuffer buffer, final int offset, final int length) {
        this.buffer.wrap(buffer, offset, length);
    }

    public int getId() {
        return this.buffer.getInt(0);
    }

    public void setId(final int id) {
        this.buffer.putInt(0, id);
    }
}
