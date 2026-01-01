package unsafeSlab;

public interface UnsafeCodec {

    short bufferSize();

    void wrap(final long memOffset);

    int keyOffset();

    int keyLength();
}
