package onthego.database.core.serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class BooleanSerializer implements Serializer<Object> {
    @Override
    public void write(DataOutputStream out, Object obj) throws IOException {
        //if (obj == null) throw new IOException("Cannot serialize null value.");
        if (obj == null) out.writeBoolean(Boolean.FALSE);
        else out.writeBoolean((Boolean)obj);
    }

    @Override
    public Object read(DataInputStream in) throws IOException {
        return Boolean.toString(in.readBoolean());
    }

    @Override
    public int estimateSize(Object obj) {
        return 1;
    }
}
