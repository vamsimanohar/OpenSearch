import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TestCDataTransfer {
    public static void main(String[] args) {
        RootAllocator root1 = new RootAllocator(Long.MAX_VALUE);
        RootAllocator root2 = new RootAllocator(Long.MAX_VALUE);

        BufferAllocator datafusionAlloc = root1.newChildAllocator("datafusion", 0, Long.MAX_VALUE);
        BufferAllocator flightAlloc = root2.newChildAllocator("flight", 0, Long.MAX_VALUE);

        // Create source root under datafusion allocator
        Schema schema = new Schema(List.of(
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        VectorSchemaRoot sourceRoot = VectorSchemaRoot.create(schema, datafusionAlloc);
        VarCharVector names = (VarCharVector) sourceRoot.getVector("name");
        IntVector ages = (IntVector) sourceRoot.getVector("age");
        names.allocateNew();
        ages.allocateNew();
        names.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
        names.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
        ages.setSafe(0, 30);
        ages.setSafe(1, 31);
        sourceRoot.setRowCount(2);

        System.out.println("Before C Data Interface transfer:");
        System.out.println("  datafusion allocated: " + datafusionAlloc.getAllocatedMemory());
        System.out.println("  flight allocated: " + flightAlloc.getAllocatedMemory());

        try {
            // Export from datafusion allocator
            ArrowArray arrowArray = ArrowArray.allocateNew(datafusionAlloc);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(datafusionAlloc);
            Data.exportVectorSchemaRoot(datafusionAlloc, sourceRoot, null, arrowArray, arrowSchema);

            // Import into flight allocator
            VectorSchemaRoot targetRoot = Data.importVectorSchemaRoot(flightAlloc, arrowArray, arrowSchema, null);

            System.out.println("\nAfter C Data Interface transfer — SUCCESS:");
            System.out.println("  datafusion allocated: " + datafusionAlloc.getAllocatedMemory());
            System.out.println("  flight allocated: " + flightAlloc.getAllocatedMemory());
            System.out.println("  target rows: " + targetRoot.getRowCount());
            VarCharVector tNames = (VarCharVector) targetRoot.getVector("name");
            IntVector tAges = (IntVector) targetRoot.getVector("age");
            System.out.println("  name[0]=" + new String(tNames.get(0)) + " age[0]=" + tAges.get(0));
            System.out.println("  name[1]=" + new String(tNames.get(1)) + " age[1]=" + tAges.get(1));

            targetRoot.close();
            arrowArray.close();
            arrowSchema.close();
        } catch (Exception e) {
            System.out.println("\nC Data Interface transfer FAILED: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            e.printStackTrace();
        }

        sourceRoot.close();
        datafusionAlloc.close();
        flightAlloc.close();
        root1.close();
        root2.close();
    }
}
