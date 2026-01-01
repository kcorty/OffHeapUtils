package unsafeSlab;

public class UnsafeTestOrder implements UnsafeCodec {

    private long memOffset;

    @Override
    public short bufferSize() {
        return 256;
    }

    @Override
    public void wrap(final long memOffset) {
        this.memOffset = memOffset;
    }

    @Override
    public int keyOffset() {
        return 0;
    }

    @Override
    public int keyLength() {
        return 0;
    }
}
