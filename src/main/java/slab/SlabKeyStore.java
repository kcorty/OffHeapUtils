package slab;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.collections.Hashing;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class SlabKeyStore<T extends Codec> {

    private final UnsafeBuffer buffer;
    private final float loadFactor;
    private final Slab<T> slab;

    private int capacity;
    private int nextResizeLimit;
    private int size;

    private static final int MISSING_VALUE = -1;

    public SlabKeyStore(final int capacity, final float loadFactor, final Slab<T> slab) {
        this.capacity = BitUtil.findNextPositivePowerOfTwo(capacity);
        this.loadFactor = loadFactor;
        this.nextResizeLimit = (int) (this.capacity * loadFactor);
        this.slab = slab;

        this.buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Integer.BYTES * this.capacity));
        this.buffer.setMemory(0, this.buffer.capacity(), (byte) MISSING_VALUE);
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public void insert(final int slabIndex, final T codec) {
        final int mask = this.capacity - 1;
        int index = Hashing.hash(codec.keyHashCode(), mask);
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

    public int wrapFromKey(final DirectBuffer lookupBuffer, final int bufferOffset,
                           final int hashCode) {
        final int mask = this.capacity - 1;
        int index = Hashing.hash(hashCode, mask);
        int existingSlabIndex;
        while ((existingSlabIndex = buffer.getInt(index << 2)) != -1) {
            if (slab.equalsUnderlying(existingSlabIndex, lookupBuffer, bufferOffset)) {
                return existingSlabIndex;
            }
            index = ++index & mask;
        }
        return MISSING_VALUE;
    }

    public int wrapFromKey(final T codec) {
        final int mask = this.capacity - 1;
        int index = Hashing.hash(codec.keyHashCode(), mask);
        int existingSlabIndex;
        while ((existingSlabIndex = buffer.getInt(index << 2)) != MISSING_VALUE) {
            if (slab.equalsUnderlying(existingSlabIndex, codec.buffer(), codec.keyOffset())) {
                return existingSlabIndex;
            }
            index = ++index & mask;
        }
        return MISSING_VALUE;
    }

    public int removeCodec(final T codec) {
        final int mask = this.capacity - 1;
        int index = Hashing.hash(codec.keyHashCode(), mask);
        int existingSlabIndex;
        while ((existingSlabIndex = buffer.getInt(index << 2)) != MISSING_VALUE) {
            if (slab.equalsUnderlying(existingSlabIndex, codec.buffer(), codec.keyOffset())) {
                buffer.putInt(index << 2, MISSING_VALUE);
                size--;
                tryCompact(index);
                return existingSlabIndex;
            }
            index = ++index & mask;
        }
        return MISSING_VALUE;
    }

    public int removeFromKey(final DirectBuffer lookupBuffer, final int bufferOffset,
                             final int hashCode) {
        final int mask = this.capacity - 1;
        int index = Hashing.hash(hashCode, mask);
        int existingSlabIndex;
        while ((existingSlabIndex = buffer.getInt(index << 2)) != MISSING_VALUE) {
            if (slab.equalsUnderlying(existingSlabIndex, lookupBuffer, bufferOffset)) {
                this.buffer.putInt(index << 2, MISSING_VALUE);
                size--;
                tryCompact(index);
                return existingSlabIndex;
            }
            index = ++index & mask;
        }
        return MISSING_VALUE;
    }

    public boolean removeAt(final int slabIndex) {
        final int mask = this.capacity - 1;
        int index = Hashing.hash(slab.keyHashCode(slabIndex), mask);
        int existingSlabIndex;
        while ((existingSlabIndex = buffer.getInt(index << 2)) != MISSING_VALUE) {
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

    public boolean remove(final int slabIndex, final T codec) {
        final int mask = this.capacity - 1;
        int index = Hashing.hash(codec.keyHashCode(), mask);
        int existingSlabIndex;
        while ((existingSlabIndex = buffer.getInt(index << 2)) != MISSING_VALUE) {
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
        newBuffer.setMemory(0, newBuffer.capacity(), (byte) MISSING_VALUE);
        final int mask = capacity - 1;
        this.nextResizeLimit = (int) (this.capacity * loadFactor);
        for (int readIndex = 0; readIndex < oldCapacity; readIndex++) {
            final int value = buffer.getInt(readIndex << 2);
            if (value != MISSING_VALUE) {
                int index = Hashing.hash(slab.keyHashCode(value), mask);
                while (newBuffer.getInt(index << 2) != MISSING_VALUE) {
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
            final int newValue = buffer.getInt(index << 2);
            if (newValue == MISSING_VALUE) {
                return;
            }
            final int hash = Hashing.hash(slab.keyHashCode(newValue), mask);

            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index)) ||
                    (hash <= deleteIndex && deleteIndex <= index)) {

                buffer.putInt(deleteIndex << 2, newValue);
                buffer.putInt(index << 2, MISSING_VALUE);
                deleteIndex = index;
            }
        }
    }

    public String printDataStore() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        for (int i = 0; i < capacity; i++) {
            stringBuilder.append(buffer.getInt(i << 2));
            if (i != capacity - 1) {
                stringBuilder.append(", ");
            }
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
}
