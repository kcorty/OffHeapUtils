package offHeapMutableAsciiString;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Arrays;
import java.util.InputMismatchException;

public class UnsafeByteString {

    private final MutableDirectBuffer buffer;

    public UnsafeByteString(final int size) {
        if ((size & 7) != 0) {
            throw new InputMismatchException("Buffer size must be word aligned to 8 bytes!");
        }
        this.buffer = new UnsafeBuffer(new byte[size]);
    }

    public void set(final CharSequence charSequence) {
        if (charSequence.length() > buffer.capacity()) {
            throw new IndexOutOfBoundsException("CharSequence too long!");
        }
        int i = 0;
        for (; i < charSequence.length(); i++) {
            this.buffer.putChar(i, charSequence.charAt(i));
        }
        if (charSequence.length() == buffer.capacity()) {
            return;
        }
        padRemainder(charSequence.length());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(buffer.byteArray());
    }


    public void padRemainder(final int beginOffset) {
        final int length = this.buffer.capacity();
        int i = beginOffset;
        for (; i + 8 <= length; i += 8) {
            buffer.putLong(i, 0);
        }

        if (length - i >= 4) {
            buffer.putInt(i, 0);
        }
        while (i < length) {
            buffer.putByte(i++, (byte) 0);
        }
    }

}
