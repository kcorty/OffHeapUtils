package offHeapMutableAsciiString;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class HashcodeBenchmarkTests {
    final UnsafeAsciiString unsafeAsciiString1 = new UnsafeAsciiString(40);

    final MemorySegmentAsciiString segment1 = new  MemorySegmentAsciiString(40);

    String string1 = null;
    private String stringBase = "ABCDEFGHIJKLMNOPQRSTUVQXYZ";
    int i = 0;

    final UnsafeByteString unsafeByteString1 = new UnsafeByteString(40);

    @Setup(Level.Trial)
    public void setup() {
        i = 0;
        unsafeAsciiString1.set("ABCDEFGHIJKLMNOPQRSTUVQXYZ1234567891");
        string1 = "ABCDEFGHIJKLMNOPQRSTUVQXYZ1";
        segment1.set("ABCDEFGHIJKLMNOPQRSTUVQXYZ123456789");
        unsafeByteString1.set("ABCDEFGHIJKLMNOPQRSTUVQXYZ1234567891");
    }

    @Benchmark
    public void testUnsafeHash(final Blackhole blackhole) {
        string1 = stringBase + i;
        blackhole.consume(unsafeAsciiString1.hashCode());
    }

    @Benchmark
    public void testStringHash(final Blackhole blackhole) {
        string1 = stringBase + i;
        blackhole.consume(string1.hashCode());
    }

    @Benchmark
    public void testMemorySegmentHash(final Blackhole blackhole) {
        string1 = stringBase + i;
        blackhole.consume(segment1.hashCode());
    }

    @Benchmark
    public void testUnsafeByteHash(final Blackhole blackhole) {
        string1 = stringBase + i;
        blackhole.consume(unsafeByteString1.hashCode());
    }
}
