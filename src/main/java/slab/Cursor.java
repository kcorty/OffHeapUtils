package slab;

import org.agrona.collections.IntArrayQueue;


public class Cursor<T extends Codec> {

    private final IntArrayQueue cleanPageIndices;
    private SlabPage<T>[] pages;

    private SlabPage<T> currentPage;
    private final Runnable pageGenerator;

    private int nextPageIndex = 0;
    private int currPageIndex;
    public final int pageElementCount;

    public Cursor(final int pageElementCount, final IntArrayQueue cleanPageIndices,
                  final Runnable pageGenerator, final SlabPage<T>[] pages) {
        this.pageElementCount = pageElementCount;
        this.cleanPageIndices = cleanPageIndices;
        this.pageGenerator = pageGenerator;
        this.pages = pages;
        final int currIndex = cleanPageIndices.pollInt();
        this.currentPage = pages[currIndex];
        this.currPageIndex = currentPage.getPageIndex() * pageElementCount;
    }

    protected void setPages(final SlabPage<T>[] pages) {
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

    protected SlabPage<T> getCurrentPage() {
        return this.currentPage;
    }

}
