package unsafeSlab;

import org.agrona.collections.IntArrayQueue;

public class UnsafeCursor<T extends UnsafeCodec> {

    public final int pageElementCount;
    private final IntArrayQueue cleanPageIndices;
    private final Runnable pageGenerator;
    private UnsafeSlabPage<T>[] pages;
    private UnsafeSlabPage<T> currentPage;
    private int nextPageIndex = 0;
    private int currPageIndex;

    public UnsafeCursor(final int pageElementCount, final IntArrayQueue cleanPageIndices,
                        final Runnable pageGenerator, final UnsafeSlabPage<T>[] pages) {
        this.pageElementCount = pageElementCount;
        this.cleanPageIndices = cleanPageIndices;
        this.pageGenerator = pageGenerator;
        this.pages = pages;
        final int currIndex = cleanPageIndices.pollInt();
        this.currentPage = pages[currIndex];
        this.currPageIndex = currentPage.getPageIndex() * pageElementCount;
    }

    protected void setPages(final UnsafeSlabPage<T>[] pages) {
        this.pages = pages;
    }

    public void incrementCursor() {
        if (++nextPageIndex < this.pageElementCount) {
            return;
        }
        if (cleanPageIndices.isEmpty()) {
            pageGenerator.run();
        }
        final int newIndex = cleanPageIndices.pollInt();
        this.currentPage = pages[newIndex];
        this.currPageIndex = currentPage.getPageIndex() * pageElementCount;
        nextPageIndex = 0;
    }

    public int getCursorIndex() {
        return currPageIndex + nextPageIndex;
    }

    public void wrapAtCursor(final T codec) {
        this.currentPage.createAt(nextPageIndex, codec);
    }

    protected UnsafeSlabPage<T> getCurrentPage() {
        return this.currentPage;
    }
}
