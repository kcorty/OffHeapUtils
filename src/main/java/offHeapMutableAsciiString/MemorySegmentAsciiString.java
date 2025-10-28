package offHeapMutableAsciiString;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.InputMismatchException;

public class MemorySegmentAsciiString {

    public final MemorySegment memorySegment;

    public MemorySegmentAsciiString(final int size) {
        if ((size & 7) != 0) {
            throw new InputMismatchException("Buffer size must be word aligned to 8 bytes!");
        }
        this.memorySegment = Arena.ofAuto().allocate(size, 8);
    }

    public MemorySegmentAsciiString(final MemorySegment memorySegment, final int offset, final int size) {
        if ((size & 7) != 0) {
            throw new InputMismatchException("Buffer size must be word aligned to 8 bytes!");
        }
        this.memorySegment =  memorySegment.asSlice(offset, size, 1);
    }

    public void set(final CharSequence charSequence) {
        if (charSequence.length() > memorySegment.byteSize()) {
            throw new IndexOutOfBoundsException("CharSequence too long!");
        }
        int i = 0;
        for (; i < charSequence.length(); i++) {
            this.memorySegment.set(ValueLayout.JAVA_BYTE, i, (byte) charSequence.charAt(i));
        }
        if (charSequence.length() == memorySegment.byteSize()) {
            return;
        }
        padRemainder(i);
    }

    public void set(final MemorySegmentAsciiString other) {
        if (other.memorySegment.byteSize() > this.memorySegment.byteSize()) {
            throw new IndexOutOfBoundsException("Buffer too long!");
        }
        this.memorySegment.set(ValueLayout.ADDRESS, 0, other.memorySegment);
        if (other.memorySegment.byteSize() == this.memorySegment.byteSize()) {
            return;
        }
        padRemainder(other.memorySegment.byteSize());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        for (int i = 0; i + 8 <= memorySegment.byteSize(); i += 8) {
            final var value = this.memorySegment.get(ValueLayout.JAVA_LONG, i);
            if (value == 0) {
                return hashCode;
            }
            hashCode = (hashCode << 5) - hashCode + Long.hashCode(value);
        }
        return hashCode;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object instanceof final MemorySegmentAsciiString other) {
            if (this.memorySegment.byteSize() <= other.memorySegment.byteSize()) {
                return memorySegmentEquals(this.memorySegment, other.memorySegment);
            }
            return memorySegmentEquals(other.memorySegment, this.memorySegment);
        }
        return false;
    }

    public static boolean memorySegmentEquals(final MemorySegment a, final MemorySegment b) {
        for (int i = 0; i + 8 <= a.byteSize(); i += 8) {
            if (a.get(ValueLayout.JAVA_LONG, i) != b.get(ValueLayout.JAVA_LONG, i)) {
                return false;
            }
        }
        return true;
    }

    //Any allocation here is transient to startup, there is no continuous allocation post JIT
    public static boolean vectorEquals(final MemorySegment memorySegmentA, final MemorySegment memorySegmentB) {
        final LongVector a = LongVector.fromMemorySegment(VectorSpecies.ofPreferred(long.class), memorySegmentA, 0, ByteOrder.LITTLE_ENDIAN);
        final LongVector b = LongVector.fromMemorySegment(VectorSpecies.ofPreferred(long.class), memorySegmentB, 0, ByteOrder.LITTLE_ENDIAN);
        return a.eq(b).allTrue();
    }

    public void padRemainder(final long beginOffset) {
        final long length = this.memorySegment.byteSize();
        long i = beginOffset;
        while ((i & 7) != 0) {
            this.memorySegment.set(ValueLayout.JAVA_BYTE, i++, (byte) 0);
        }
        for (; i + 8 <= length; i += 8) {
            this.memorySegment.set(ValueLayout.JAVA_LONG, i, 0);
        }
    }

    @Override
    public String toString() {
        return this.memorySegment.getString(0);
    }
}
