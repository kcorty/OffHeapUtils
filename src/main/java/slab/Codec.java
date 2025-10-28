package slab;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public interface Codec {

    short bufferSize();

    void wrap(final MutableDirectBuffer buffer, final int offset, final int length);

    DirectBuffer buffer();

    int keyHashCode();
}
