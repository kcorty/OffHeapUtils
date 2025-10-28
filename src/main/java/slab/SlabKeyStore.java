package slab;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class SlabKeyStore<T extends Codec> {

    private final UnsafeBuffer buffer;
    private final float loadFactor;
    private final short codecKeyOffset;
    private final short codecKeySize;
    private final Slab<T> slab;

    private int capacity;
    private int nextResizeLimit;
    private int size;

    private static final int MISSING_VALUE = -1;

    public SlabKeyStore(final int capacity, final float loadFactor, final short codecKeyOffset, final short codecKeySize,
                        final Slab<T> slab) {
        this.capacity = BitUtil.findNextPositivePowerOfTwo(capacity);
        this.loadFactor = loadFactor;
        this.nextResizeLimit = (int) (this.capacity * loadFactor);
        this.codecKeyOffset = codecKeyOffset;
        this.codecKeySize = codecKeySize;
        this.slab = slab;

        this.buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Integer.BYTES * capacity));
        this.buffer.setMemory(0, this.buffer.capacity(), (byte) -1);
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public void insert(final int slabIndex, final T codec) {
        final int mask = this.capacity - 1;
        int index = codec.keyHashCode() & mask;
        int existingSlabIndex;
        while ((existingSlabIndex = buffer.getInt(index << 2)) != -1) {
            if (existingSlabIndex == slabIndex) {
                return;
            }
            index = ++index & mask;
        }

        size++;
        buffer.putInt(index << 2, slabIndex);
        tryIncreaseCapacity();
    }

    public boolean wrapFromKey(final DirectBuffer buffer, final int bufferOffset, final T codec) {
        final int mask = this.capacity - 1;
        int index = codec.keyHashCode() & mask;
        int existingSlabIndex;
        while ((existingSlabIndex = buffer.getInt(index << 2)) != -1) {
            slab.getAt(existingSlabIndex, codec);
            if (BufferUtils.bufferEquals(buffer, bufferOffset, codec.buffer(), codecKeyOffset, codecKeySize)) {
                return true;
            }
            index = ++index & mask;
        }
        return false;
    }

    public boolean removeCodec(final T codec) {

        return false;
    }

    public boolean removeAt(final int slabIndex) {
        final T codec = slab.get(slabIndex);
        return remove(slabIndex, codec);
    }

    public boolean remove(final int slabIndex, final T codec) {
        final int mask = this.capacity - 1;
        int index = codec.keyHashCode() & mask;
        int existingSlabIndex;
        while ((existingSlabIndex = buffer.getInt(index << 2)) != -1) {
            if (existingSlabIndex == slabIndex) {
                buffer.putInt(index << 2, MISSING_VALUE);
                size--;
                tryCompact(index);
                return true;
            }
            index = ++index & mask;
        }
        return false;
    }

    private void tryIncreaseCapacity() {
        if (size > nextResizeLimit) {
            rehash();
        }
    }

    private void rehash() {
        final int oldCapacity = this.capacity;
        this.capacity <<= 1;

        final UnsafeBuffer newBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(capacity * Integer.BYTES));
        newBuffer.setMemory(0, newBuffer.capacity(), (byte) -1);
        final int mask = capacity - 1;
        this.nextResizeLimit = (int) (this.capacity * loadFactor);
        for (int readOffset = 0; readOffset < oldCapacity; readOffset += 4) {
            final int value = buffer.getInt(readOffset);
            if (value != MISSING_VALUE) {
                final T codec = slab.get(value);
                if (codec == null) {
                    //Somebody did something wrong
                    continue;
                }
                int index = codec.keyHashCode() & mask;
                while (newBuffer.getInt(index << 2) != -1) {
                    index = ++index & mask;
                }
                newBuffer.putInt(index << 2, value);
            }
        }
        this.buffer.wrap(newBuffer, 0, newBuffer.capacity());
    }

    private void tryCompact(int deleteIndex) {
        final int mask = capacity - 1;
        int index = deleteIndex;

        while (true) {
            index = ++index & mask;
            final int value = buffer.getInt(index << 2);
            if (value == MISSING_VALUE) {
                return;
            }

            final T codec = slab.get(value);
            final int hash = codec.keyHashCode() & mask;

            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index)) ||
                    (hash <= deleteIndex && deleteIndex <= index)) {

                buffer.putInt(deleteIndex << 2, value);
                deleteIndex = index;
            }
        }
    }
}
