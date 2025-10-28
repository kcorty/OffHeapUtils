package slab;

import offHeapMutableAsciiString.UnsafeAsciiString;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class ConcreteTestOrder extends TestOrder {

    private final UnsafeAsciiString unsafeAsciiString;

    public ConcreteTestOrder() {
        this.buffer().wrap(new UnsafeBuffer(ByteBuffer.allocateDirect(this.bufferSize())));
        this.unsafeAsciiString = new UnsafeAsciiString(this.buffer(), 0, 40);
    }

    public UnsafeAsciiString getUnsafeAsciiString() {
        return this.unsafeAsciiString;
    }

}
