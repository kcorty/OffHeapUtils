package slab;

import org.agrona.concurrent.UnsafeBuffer;

public class SlabPage<T extends Codec> {

    private final UnsafeBuffer pageBuffer;
    private final short codecSize;
    private final short unitSize;
    private short liveCounter = 0;
    private final int pageIndex;

    protected static final int SLAB_PAGE_LIVE_PADDING_SIZE = 1;

    public SlabPage(final UnsafeBuffer parentBuffer, final short codecSize, final int index) {
        this.pageBuffer = parentBuffer;
        this.codecSize = codecSize;
        this.unitSize = (short) (codecSize + SLAB_PAGE_LIVE_PADDING_SIZE);
        this.pageIndex = index;
    }

    public void createAt(final int index, final T codec) {
        final int pageOffset = getOffset(index);
        liveCounter++;
        this.pageBuffer.putByte(pageOffset, (byte) 1);
        codec.wrap(pageBuffer, pageOffset + SLAB_PAGE_LIVE_PADDING_SIZE, codecSize);
    }

    public void getAt(final int index, final T codec) {
        final int codecOffset = getOffset(index) + SLAB_PAGE_LIVE_PADDING_SIZE;
        codec.wrap(pageBuffer, codecOffset, codecSize);
    }

    public int removeAt(final int index) {
        final int pageOffset = getOffset(index);
        if (pageBuffer.getByte(pageOffset) != 0) {
            liveCounter--;
            this.pageBuffer.putByte(pageOffset, (byte) 0);
        }
        return liveCounter;
    }

    protected void cleanPage() {
        liveCounter = 0;
        BufferUtils.resetBuffer(pageBuffer);
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public int getLiveCount() {
        return liveCounter;
    }

    private int getOffset(final int index) {
        return index * unitSize;
    }

    @Override
    public String toString() {
        return "SlabPage{Index=" + this.getPageIndex() + ", Counter=" + this.getLiveCount() + '}';
    }
}
