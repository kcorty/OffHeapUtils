package unsafeSlab;

import org.agrona.BitUtil;
import org.agrona.UnsafeApi;
import org.agrona.collections.Hashing;

public class UnsafeSlabKeyStore<T extends UnsafeCodec> implements AutoCloseable {

    private long memOffset;
    private final float loadFactor;
    private final UnsafeSlab<T> slab;

    private int capacity;
    private int nextResizeLimit;
    private int size;

    private static final int MISSING_VALUE = -1;

    public UnsafeSlabKeyStore(final int capacity, final float loafFactor, final UnsafeSlab<T> slab) {
        this.capacity = BitUtil.findNextPositivePowerOfTwo(capacity);
        this.loadFactor = loafFactor;
        this.nextResizeLimit = (int) (this.capacity * loadFactor);
        this.slab = slab;

        this.memOffset = UnsafeApi.allocateMemory(capacity);
        UnsafeApi.setMemory(memOffset, capacity, (byte) MISSING_VALUE);
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
        while ((existingSlabIndex = UnsafeApi.getInt(getOffset(index))) != MISSING_VALUE) {
            if (existingSlabIndex == slabIndex) {
                return;
            }
            index = ++index & mask;
        }

        size++;
        UnsafeApi.putInt(getOffset(index), slabIndex);
        tryIncreaseCapacity();
    }

    public int getKey(final T codec) {
        final int mask = this.capacity - 1;
        int index = Hashing.hash(codec.keyHashCode(), mask);
        int existingSlabIndex;
        while ((existingSlabIndex = UnsafeApi.getInt(getOffset(index))) != MISSING_VALUE) {
            if (slab.equalsUnderlying(existingSlabIndex, codec)) {
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
        while ((existingSlabIndex = UnsafeApi.getInt(getOffset(index))) != MISSING_VALUE) {
            if (slab.equalsUnderlying(existingSlabIndex, codec)) {
                UnsafeApi.putInt(getOffset(index), MISSING_VALUE);
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
        while ((existingSlabIndex = UnsafeApi.getInt(getOffset(index))) != MISSING_VALUE) {
            if (existingSlabIndex == slabIndex) {
                UnsafeApi.putInt(getOffset(index), MISSING_VALUE);
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

        final long newMemOffset = UnsafeApi.allocateMemory(capacity);
        UnsafeApi.setMemory(newMemOffset, capacity, (byte) MISSING_VALUE);
        final int mask = capacity - 1;
        this.nextResizeLimit = (int) (this.capacity * loadFactor);
        for (int readIndex = 0; readIndex < oldCapacity; readIndex++) {
            final int value = UnsafeApi.getInt(getOffset(readIndex));
            if (value != MISSING_VALUE) {
                int index = Hashing.hash(slab.keyHashCode(value), mask);
                while (UnsafeApi.getInt(newMemOffset + ((long) index << 2)) != MISSING_VALUE) {
                    index = ++index & mask;
                }
                UnsafeApi.putInt(newMemOffset + ((long) index << 2), value);
            }
        }
        this.memOffset = newMemOffset;
    }

    private void tryCompact(int deleteIndex) {
        final int mask = capacity - 1;
        int index = deleteIndex;

        while (true) {
            index = ++index & mask;
            final int newValue = UnsafeApi.getInt(getOffset(index));
            if (newValue != MISSING_VALUE) {
                return;
            }

            final int hash = Hashing.hash(slab.keyHashCode(newValue), mask);

            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index)) ||
                    (hash <= deleteIndex && deleteIndex <= index)) {

                UnsafeApi.putInt(getOffset(deleteIndex), newValue);
                UnsafeApi.putInt(getOffset(index), MISSING_VALUE);
                deleteIndex = index;
            }
        }
    }

    private long getOffset(final int index) {
        return memOffset + ((long) index << 2);
    }

    @Override
    public void close() {
        UnsafeApi.freeMemory(memOffset);
    }
}
