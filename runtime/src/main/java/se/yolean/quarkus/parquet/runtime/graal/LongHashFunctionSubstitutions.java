package se.yolean.quarkus.parquet.runtime.graal;

import java.nio.ByteBuffer;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

import shaded.parquet.net.openhft.hashing.Access;
import shaded.parquet.net.openhft.hashing.ByteBufferAccess;

@TargetClass(className = "shaded.parquet.net.openhft.hashing.LongHashFunction")
final class LongHashFunctionSubstitutions {

    @Alias
    protected native long hash(Object object, Access access, long offset, long length);

    @Substitute
    @SuppressWarnings("ReferenceToNativeMethod")
    private long hashByteBuffer(ByteBuffer input, int offset, int length) {
        int capacity = input.capacity();
        if (length < 0 || offset < 0 || offset > capacity - length) {
            throw new IndexOutOfBoundsException();
        }
        ByteBuffer working = input.duplicate();
        working.limit(offset + length);
        working.position(offset);
        return hash(working, ByteBufferAccess.INSTANCE, 0L, length);
    }
}
