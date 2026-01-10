package unsafeSlab;

import static unsafeSlab.UnsafeSlabPage.SLAB_PAGE_LIVE_PADDING_SIZE;

import java.util.function.Supplier;
import org.agrona.BitUtil;
import org.agrona.UnsafeApi;
import org.agrona.collections.IntArrayQueue;

public class UnsafeSlab<T extends UnsafeCodec> implements AutoCloseable {

    public static boolean RESET_BUFFER = !"true".equals(System.getProperty("slab.reset.buffer"));
    private final long memOffset;
    private final IntArrayQueue cleanPageIndices;
    private final UnsafeCursor<T> cursor;
    private final int inPageIndexMask;
    private final int shiftCount;
    private final T reusableCodec;
    private final int singlePageSize;
    private int activePageCount = 0;
    private UnsafeSlabPage<T>[] pages;

    public UnsafeSlab(final short pageSize, final int initialPageCount, final Supplier<T> codecSupplier) {
        this.reusableCodec = codecSupplier.get();
        final short codecSize = reusableCodec.bufferSize();
        final short alignedPageElementCount = (short) BitUtil.findNextPositivePowerOfTwo(pageSize);
        this.inPageIndexMask = alignedPageElementCount - 1;
        int pageIndexMask = inPageIndexMask;
        int shiftCount = 1;
        while ((pageIndexMask >>= 1) != 0) {
            shiftCount++;
        }
        this.shiftCount = shiftCount;

        this.singlePageSize = alignedPageElementCount * (codecSize + SLAB_PAGE_LIVE_PADDING_SIZE);
        memOffset = UnsafeApi.allocateMemory((long) initialPageCount * singlePageSize);
        this.cleanPageIndices = new IntArrayQueue(Math.max(initialPageCount, IntArrayQueue.MIN_CAPACITY), -1);
        this.pages = new UnsafeSlabPage[initialPageCount];
        for (int i = 0; i < initialPageCount; i++) {
            addPage(memOffset + ((long) i * singlePageSize), i, false);
        }
        this.cursor = new UnsafeCursor<>(alignedPageElementCount, cleanPageIndices, this::addPage, pages);
    }

    private void addPage() {
        final long allocOffset = UnsafeApi.allocateMemory(singlePageSize);
        if (activePageCount + 1 > pages.length) {
            final UnsafeSlabPage<T>[] newPages = new UnsafeSlabPage[pages.length * 2];
            System.arraycopy(pages, 0, newPages, 0, pages.length);
            pages = newPages;
            this.cursor.setPages(pages);
        }
        addPage(allocOffset, activePageCount, true);
    }

    private void addPage(final long memOffset, final int index, final boolean isOwnAlloc) {
        final UnsafeSlabPage<T> slabPage = new UnsafeSlabPage<>(memOffset, reusableCodec.bufferSize(),
                activePageCount++, singlePageSize, isOwnAlloc);
        pages[index] = slabPage;
        this.cleanPageIndices.addInt(index);
    }

    public int create(final T codec) {
        cursor.wrapAtCursor(codec);
        final int newIndex = cursor.getCursorIndex();
        cursor.incrementCursor();
        return newIndex;
    }

    public void getAt(final int index, final T codec) {
        final var inPageIndex = index & inPageIndexMask;
        final var pageIndex = index >> shiftCount;
        final var page = pages[pageIndex];
        page.getAt(inPageIndex, codec);
    }

    public T get(final int index) {
        final var inPageIndex = index & inPageIndexMask;
        final var pageIndex = index >> shiftCount;
        final var page = pages[pageIndex];
        page.getAt(inPageIndex, reusableCodec);
        return reusableCodec;
    }

    public boolean equalsUnderlying(final int index, final T codec) {
        final int inPageIndex = index & inPageIndexMask;
        final int pageIndex = index >> shiftCount;
        final var page = pages[pageIndex];
        return page.equalsUnderlying(inPageIndex, codec);
    }

    public int keyHashCode(final int index) {
        final int inPageIndex = index & inPageIndexMask;
        final int pageIndex = index >> shiftCount;
        final var page = pages[pageIndex];
        return page.keyHashCode(inPageIndex, reusableCodec);
    }

    public void removeAt(final int index) {
        final var inPageIndex = index & inPageIndexMask;
        final var pageIndex = index >> shiftCount;
        final var page = pages[pageIndex];
        final int remainingCount = page.removeAt(inPageIndex);
        if (page == cursor.getCurrentPage() || remainingCount != 0) {
            return;
        }
        freePage(page);
    }

    private void freePage(final UnsafeSlabPage<T> slabPage) {
        if (RESET_BUFFER) {
            slabPage.cleanPage();
        }
        cleanPageIndices.addInt(slabPage.getPageIndex());
    }

    @Override
    public void close() {
        for (final var page : pages) {
            if (page != null) {
                page.tryFree();
            }
        }
        UnsafeApi.freeMemory(memOffset);
    }
}
