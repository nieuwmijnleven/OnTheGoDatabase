package onthego.database.util;

import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

@UtilityClass
public class IOUtils {
    public ByteBuffer getByteBuffer(int capacity, boolean flip, Consumer<ByteBuffer> operations) {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        if (operations != null) operations.accept(buffer);
        if (flip) buffer.flip();
        return buffer;
    }
}
