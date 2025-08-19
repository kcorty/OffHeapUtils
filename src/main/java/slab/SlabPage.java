package slab;

import org.agrona.concurrent.UnsafeBuffer;

public class SlabPage<T extends Codec> {

    private final UnsafeBuffer pageBuffer;
    private final short codecSize;
    private final short unitSize;

    protected static final int SLAB_PAGE_HEADER_SIZE = 8;
    protected static final int SLAB_PAGE_LIVE_PADDING_SIZE = 1;

    private static int offset = 0;
    protected static final int SLAB_PAGE_INDEX_STORE_OFFSET = offset;
    protected static final int SLAB_PAGE_COUNTER_OFFSET = offset += 4;

    public SlabPage(final UnsafeBuffer parentBuffer, final short codecSize,
                    final int index) {
        this.pageBuffer = parentBuffer;
        this.codecSize = codecSize;
        this.unitSize = (short) (codecSize + SLAB_PAGE_LIVE_PADDING_SIZE);
        this.pageBuffer.putInt(SLAB_PAGE_INDEX_STORE_OFFSET, index);
    }

    public void createAt(final int index, final T codec) {
        final int pageOffset = getOffset(index);
        this.pageBuffer.getAndAddInt(SLAB_PAGE_COUNTER_OFFSET, 1);
        this.pageBuffer.putByte(pageOffset, (byte) 1);
        codec.wrap(pageBuffer, pageOffset + SLAB_PAGE_LIVE_PADDING_SIZE, codecSize);
    }

    public void getAt(final int index, final T codec) {
        final int codecOffset = getOffset(index) + SLAB_PAGE_LIVE_PADDING_SIZE;
        codec.wrap(pageBuffer, codecOffset, codecSize);
    }

    public int removeAt(final int index) {
        final int pageOffset = getOffset(index);
        int currCounter = this.pageBuffer.getInt(SLAB_PAGE_COUNTER_OFFSET);
        if (pageBuffer.getByte(pageOffset) != 0) {
            this.pageBuffer.putInt(SLAB_PAGE_COUNTER_OFFSET, --currCounter);
            this.pageBuffer.putByte(pageOffset, (byte) 0);
        }
        return currCounter;
    }

    protected void cleanPage() {
        BufferUtils.resetBuffer(pageBuffer, SLAB_PAGE_HEADER_SIZE);
    }

    public int getPageIndex() {
        return this.pageBuffer.getInt(SLAB_PAGE_INDEX_STORE_OFFSET);
    }

    public int getLiveCount() {
        return this.pageBuffer.getInt(SLAB_PAGE_COUNTER_OFFSET);
    }

    private int getOffset(final int index) {
        return SLAB_PAGE_HEADER_SIZE + (index * unitSize);
    }

    @Override
    public String toString() {
        return "SlabPage{Index=" + this.getPageIndex() + ", Counter=" + this.getLiveCount() + '}';
    }
}
