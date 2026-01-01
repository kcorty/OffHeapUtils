package unsafeSlab;

import org.agrona.UnsafeApi;

public class UnsafeSlabPage<T extends UnsafeCodec> {

    protected static final int SLAB_PAGE_LIVE_PADDING_SIZE = 1;
    private final long memOffset;
    private final short unitSize;
    private final boolean isOwnAlloc;
    private final int pageIndex;
    private final int singlePageSize;
    private short liveCounter = 0;

    public UnsafeSlabPage(final long memOffset, final short codecSize, final int index,
                          final int singlePageSize, final boolean isOwnAlloc) {
        this.memOffset = memOffset;
        this.unitSize = (short) (codecSize + SLAB_PAGE_LIVE_PADDING_SIZE);
        this.pageIndex = index;
        this.singlePageSize = singlePageSize;
        this.isOwnAlloc = isOwnAlloc;
    }

    public void createAt(final int index, final T codec) {
        final long pageOffset = getOffset(index);
        liveCounter++;
        UnsafeApi.putByte(pageOffset, (byte) 1);
        codec.wrap(pageOffset + SLAB_PAGE_LIVE_PADDING_SIZE);
    }

    public void getAt(final int index, final T codec) {
        final long codecOffset = getOffset(index) + SLAB_PAGE_LIVE_PADDING_SIZE;
        codec.wrap(codecOffset);
    }

    public int removeAt(final int index) {
        final long pageOffset = getOffset(index);
        if (UnsafeApi.getByte(pageOffset) != 0) {
            liveCounter--;
            UnsafeApi.putByte(pageOffset, (byte) 0);
        }
        return liveCounter;
    }

    protected void cleanPage() {
        liveCounter = 0;
        UnsafeApi.setMemory(memOffset, singlePageSize, (byte) 0);
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public int getLiveCount() {
        return liveCounter;
    }

    private long getOffset(final int index) {
        return memOffset + ((long) index * unitSize);
    }

    protected void tryFree() {
        if (isOwnAlloc) {
            UnsafeApi.freeMemory(memOffset);
        }
    }

    @Override
    public String toString() {
        return "UnsafeSlabPage{Index=" + this.getPageIndex() + ", Counter=" + this.getLiveCount() + '}';
    }
}
