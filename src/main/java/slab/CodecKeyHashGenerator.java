package slab;

import org.agrona.MutableDirectBuffer;

public interface CodecKeyHashGenerator {

    int generateKeyHashCode(MutableDirectBuffer buffer, int offset);
}
