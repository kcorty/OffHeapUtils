package slab;

import org.agrona.collections.IntArrayQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(5)
@Warmup(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class SlabBenchmarkTests {

    private final TestOrder codec = new TestOrder();
    private Slab<TestOrder> slab = null;
    private final IntArrayQueue intArrayQueue = new IntArrayQueue(256, -1);
    private boolean isDraining;

    @Setup(Level.Iteration)
    public void setup() {
        this.slab = new Slab<>((short) 256, 4096, () -> this.codec);
        System.gc();
        intArrayQueue.clear();
        isDraining = false;
    }

    @Benchmark
    public void testSlabInsertion(final Blackhole blackhole) {
        blackhole.consume(slab.create(codec));
    }

    @Benchmark
    public void testSlabGet() {
        final int index = slab.create(codec);
        slab.getAt(index, codec);
    }

    @Benchmark
    public void testSlabRemove() {
        final int index = slab.create(codec);
        slab.removeAt(index);
    }

    @Benchmark
    public void testIncrementalRemoves() {
        if (intArrayQueue.size() == 1024) {
            isDraining = true;
        }
        if (intArrayQueue.isEmpty()) {
            isDraining = false;
        }
        if (isDraining) {
            slab.removeAt(intArrayQueue.poll());
        } else {
            intArrayQueue.addInt(slab.create(codec));
        }
    }
}
