package offHeapDataStructures;

import org.agrona.BitUtil;
import org.agrona.collections.Hashing;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.generation.DoNotSub;
import slab.BufferUtils;
import slab.Codec;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class BytesToIntOffHeapMap<T extends Codec> implements Map<T, Integer> {

    private final UnsafeBuffer buffer;
    private final float loadFactor;
    private final int singleEntrySize;
    private final short codecSize;

    private int capacity;
    @DoNotSub private int nextResizeLimit;
    @DoNotSub private int size;

    private static final int OCCUPIED_MARKER_SIZE = 1;
    private static final int INT_SIZE = 4;

    private static final int MISSING_VALUE = -1;

    public BytesToIntOffHeapMap(final Supplier<T> codecSupplier) {
        this(8, codecSupplier);
    }

    public BytesToIntOffHeapMap(final int capacity, final Supplier<T> codecSupplier) {
        this(capacity, 0.5f, codecSupplier);
    }

    public BytesToIntOffHeapMap(final int capacity, final float loadFactor, final Supplier<T> codecSupplier) {
        final int updatedCapacity = BitUtil.findNextPositivePowerOfTwo(capacity);
        this.capacity = updatedCapacity;
        this.loadFactor = loadFactor;
        this.nextResizeLimit = (int) (updatedCapacity * loadFactor);

        final var tempCodec = codecSupplier.get();
        this.codecSize = tempCodec.bufferSize();
        this.singleEntrySize = OCCUPIED_MARKER_SIZE + INT_SIZE + codecSize;
        this.buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(singleEntrySize * updatedCapacity));
    }


    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(final Object key) {
        return MISSING_VALUE != getInt(((T) key));
    }

    @Override
    public boolean containsValue(final Object value) {
        return containsValue((int) value);
    }

    public boolean containsValue(final int value) {
        for (int i = 0; i < capacity; i++) {
            final int startLocation = i * singleEntrySize;
            if (buffer.getByte(startLocation) == 0) {
                continue;
            }
            if (buffer.getInt(startLocation + OCCUPIED_MARKER_SIZE + codecSize) == value) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Integer get(Object key) {
        final int value = getInt((T) key);
        return value == MISSING_VALUE ? null : value;
    }

    public int getInt(final T codec) {
        requireNonNull(codec);
        @DoNotSub final int mask = this.capacity - 1;
        @DoNotSub int index = Hashing.hash(codec, mask);
        int offset = index * singleEntrySize;
        while (buffer.getByte(offset) != 0) {
            if (BufferUtils.bufferEquals(codec.buffer(), buffer, offset + OCCUPIED_MARKER_SIZE, codecSize)) {
                return buffer.getInt(offset + OCCUPIED_MARKER_SIZE + codecSize);
            }
            index = ++index & mask;
            offset = index * singleEntrySize;
        }
        return MISSING_VALUE;
    }

    @Override
    public Integer put(T key, Integer value) {
        return putValue(key, value);
    }

    //Try to optimize this through byte alignment
    public int putValue(final T codec, final int value) {
        requireNonNull(codec);
        @DoNotSub final int mask = capacity - 1;
        @DoNotSub int index = Hashing.hash(codec, mask);
        int offset = index * singleEntrySize;
        while (buffer.getByte(offset) != 0) {
            if (BufferUtils.bufferEquals(codec.buffer(), buffer, offset + OCCUPIED_MARKER_SIZE, codecSize)) {
                final int oldValue = buffer.getInt(offset + OCCUPIED_MARKER_SIZE + codecSize);
                buffer.putInt(offset + OCCUPIED_MARKER_SIZE + codecSize, value);
                return oldValue;
            }

            index = ++index & mask;
            offset = index * singleEntrySize;
        }

        size++;
        buffer.putByte(offset, (byte) 1);
        buffer.putBytes(offset + OCCUPIED_MARKER_SIZE, codec.buffer(), 0, codecSize);
        buffer.putInt(offset + OCCUPIED_MARKER_SIZE + codecSize, value);
        tryIncreaseCapacity();

        return MISSING_VALUE;
    }

    @Override
    public Integer remove(final Object key) {
        final int removedKey = removeKey((T) key);
        return removedKey == MISSING_VALUE ? null : removedKey;
    }

    public int removeKey(final T codec) {
        requireNonNull(codec);
        @DoNotSub final int mask = this.capacity - 1;
        @DoNotSub int index = Hashing.hash(codec, mask);
        int offset = index * singleEntrySize;
        while (buffer.getByte(offset) != 0) {
            if (BufferUtils.bufferEquals(codec.buffer(), buffer, offset + OCCUPIED_MARKER_SIZE, codecSize)) {
                //mark empty
                buffer.putByte(offset, (byte) 0);
                size--;
                tryCompact(index);
                return buffer.getInt(offset + OCCUPIED_MARKER_SIZE + codecSize);
            }
            index = ++index & mask;
            offset = index * singleEntrySize;
        }
        return MISSING_VALUE;
    }

    @Override
    public void putAll(final Map<? extends T, ? extends Integer> m) {
        for (final var entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        BufferUtils.resetBuffer(buffer);
        size = 0;
    }

    @Override
    public Set<T> keySet() {
        return Set.of();
    }

    @Override
    public Collection<Integer> values() {
        return List.of();
    }

    @Override
    public Set<Entry<T, Integer>> entrySet() {
        return Set.of();
    }

    private void tryIncreaseCapacity() {
        if (size > nextResizeLimit) {
            rehash();
        }
    }

    private void rehash() {
        final int oldCapacity = this.capacity;
        this.capacity <<= 1;
        @DoNotSub final int mask = capacity - 1;
        this.nextResizeLimit = (int) (this.capacity * loadFactor);

        final UnsafeBuffer newBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(capacity * singleEntrySize));
        int offset = 0;
        for (@DoNotSub int i = 0; i < oldCapacity; i++) {
            if (buffer.getByte(offset) != 0) {
                @DoNotSub int index = Hashing.hash(BufferUtils.segmentHashCode(buffer, offset + OCCUPIED_MARKER_SIZE, codecSize), mask);
                int newOffset = index * singleEntrySize;
                while (newBuffer.getByte(newOffset) != 0) {
                    index = ++index & mask;
                    newOffset = index * singleEntrySize;
                }

                newBuffer.putByte(newOffset, (byte) 1);
                newBuffer.putBytes(newOffset + OCCUPIED_MARKER_SIZE,
                        buffer, offset + OCCUPIED_MARKER_SIZE, codecSize);
                newBuffer.putInt(newOffset + OCCUPIED_MARKER_SIZE + codecSize,
                        buffer.getInt(offset + OCCUPIED_MARKER_SIZE + codecSize));
            }
            offset += singleEntrySize;
        }
        this.buffer.wrap(newBuffer, 0, newBuffer.capacity());
    }

    private void tryCompact(int deleteIndex) {
        @DoNotSub final int mask = capacity - 1;
        @DoNotSub int index = deleteIndex;

        while (true) {
            index = ++index & mask;
            final int offset = index * singleEntrySize;

            if (buffer.getByte(offset) == 0) {
                return;
            }

            @DoNotSub final int hash = Hashing.hash(BufferUtils.segmentHashCode(buffer, offset + OCCUPIED_MARKER_SIZE, codecSize), mask);

            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index)) ||
                    (hash <= deleteIndex && deleteIndex <= index)) {
                final int deleteOffset = deleteIndex * singleEntrySize;

                buffer.putByte(deleteOffset, (byte) 1);
                buffer.putBytes(deleteOffset + OCCUPIED_MARKER_SIZE, buffer,
                        offset + OCCUPIED_MARKER_SIZE, codecSize);
                buffer.putInt(deleteOffset + OCCUPIED_MARKER_SIZE + codecSize,
                        buffer.getInt(offset + OCCUPIED_MARKER_SIZE + codecSize));

                buffer.putByte(offset, (byte) 0);
                deleteIndex = index;
            }
        }
    }
}
