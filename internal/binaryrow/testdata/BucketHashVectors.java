// Regenerate using the actual Paimon writer/hash implementation (not Go code):
// java --class-path paimon-common-1.1.1.jar BucketHashVectors.java
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;

public class BucketHashVectors {
    static void emit(String name, Object... values) {
        BinaryRow row = new BinaryRow(values.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        for (int i = 0; i < values.length; i++) {
            Object v = values[i];
            if (v == null) writer.setNullAt(i);
            else if (v instanceof Boolean) writer.writeBoolean(i, (boolean) v);
            else if (v instanceof Byte) writer.writeByte(i, (byte) v);
            else if (v instanceof Short) writer.writeShort(i, (short) v);
            else if (v instanceof Integer) writer.writeInt(i, (int) v);
            else if (v instanceof Long) writer.writeLong(i, (long) v);
            else if (v instanceof String) writer.writeString(i, BinaryString.fromString((String) v));
            else throw new IllegalArgumentException("Unsupported value " + v);
        }
        writer.complete();
        System.out.println(name + "," + row.hashCode());
    }
    public static void main(String[] args) {
        emit("int_zero", 0);
        emit("int_one", 1);
        emit("int_negative", -1);
        emit("int_min", Integer.MIN_VALUE);
        emit("long_min", Long.MIN_VALUE);
        emit("long_large", 9007199254740993L);
        emit("byte_negative", (byte)-1);
        emit("short_negative", (short)-1);
        emit("boolean_true", true);
        emit("null", (Object)null);
        emit("empty", "");
        emit("inline", "1234567");
        emit("heap", "12345678");
        emit("unicode", "æøå😀");
        emit("composite", "profile-123", "asset-987", 42, -7L, true);
        Object[] many = new Object[65];
        for (int i = 0; i < many.length; i++) many[i] = i == 64 ? null : i;
        emit("many", many);
    }
}
