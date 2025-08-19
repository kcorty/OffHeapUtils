package slab;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class TestOrder implements Codec {

    private final MutableDirectBuffer buffer = new UnsafeBuffer();

    @Override
    public short bufferSize() {
        return 256;
    }

    @Override
    public void wrap(final MutableDirectBuffer buffer, final int offset, final int length) {
        this.buffer.wrap(buffer, offset, length);
    }


}
