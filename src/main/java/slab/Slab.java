package slab;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

import static slab.SlabPage.SLAB_PAGE_HEADER_SIZE;
import static slab.SlabPage.SLAB_PAGE_LIVE_PADDING_SIZE;

public class Slab<T extends Codec> {

    private int activePagesCount = 0;
    private SlabPage<T>[] pages;

    private final IntArrayQueue cleanPageIndices;
    private final Cursor<T> cursor;

    private final int inPageIndexMask;
    private final int shiftCount;

    private final T reusableCodec;
    private final int singlePageSize;

    public static boolean RESET_BUFFER = !"true".equals(System.getProperty("slab.reset.buffer"));

    public Slab(final short pageSize, final int initialPageCount, final Supplier<T> codecSupplier) {
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

        this.singlePageSize = SLAB_PAGE_HEADER_SIZE +
                (alignedPageElementCount * (codecSize + SLAB_PAGE_LIVE_PADDING_SIZE));
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(
                initialPageCount * singlePageSize));
        this.cleanPageIndices = new IntArrayQueue(Math.max(initialPageCount, IntArrayQueue.MIN_CAPACITY), -1);
        this.pages = new SlabPage[initialPageCount];
        for (int i = 0; i < initialPageCount; i++) {
            addPage(unsafeBuffer, i * singlePageSize, i);
        }
        this.cursor = new Cursor<>(alignedPageElementCount, cleanPageIndices, this::addPage, pages);
    }

    private void addPage() {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(singlePageSize));
        if (activePagesCount + 1 > pages.length) {
            final SlabPage<T>[] newPages = new SlabPage[pages.length * 2];
            System.arraycopy(pages, 0, newPages, 0, pages.length);
            pages = newPages;
            this.cursor.setPages(pages);
        }
        addPage(buffer, 0, activePagesCount);
    }

    private void addPage(final MutableDirectBuffer buffer, final int offset, final int index) {
        final UnsafeBuffer newBuffer = new UnsafeBuffer(buffer, offset, singlePageSize);
        final SlabPage<T> slabPage = new SlabPage<>(newBuffer, reusableCodec.bufferSize(), activePagesCount);
        pages[index] = slabPage;
        activePagesCount++;
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

    private void freePage(final SlabPage<T> slabPage) {
        if (RESET_BUFFER) {
            slabPage.cleanPage();
        }
        cleanPageIndices.addInt(slabPage.getPageIndex());
    }

}
