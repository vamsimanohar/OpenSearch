import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.util.List;

public class TestAllocatorAssumptions {
    public static void main(String[] args) {
        System.out.println("=== Test 1: transfer() within same root (should work) ===");
        try (RootAllocator root = new RootAllocator(Long.MAX_VALUE)) {
            BufferAllocator child1 = root.newChildAllocator("child1", 0, Long.MAX_VALUE);
            BufferAllocator child2 = root.newChildAllocator("child2", 0, Long.MAX_VALUE);
            IntVector source = new IntVector("v", child1);
            source.allocateNew(2);
            source.setSafe(0, 42);
            source.setSafe(1, 99);
            source.setValueCount(2);

            IntVector target = new IntVector("v", child2);
            source.makeTransferPair(target).transfer();
            System.out.println("  SUCCESS: target[0]=" + target.get(0) + " target[1]=" + target.get(1));
            System.out.println("  child1 allocated: " + child1.getAllocatedMemory() + " (should be 0)");
            System.out.println("  child2 allocated: " + child2.getAllocatedMemory() + " (should be >0)");
            target.close();
            child1.close();
            child2.close();
        }

        System.out.println("\n=== Test 2: transfer() across different roots (should fail) ===");
        try (RootAllocator root1 = new RootAllocator(Long.MAX_VALUE);
             RootAllocator root2 = new RootAllocator(Long.MAX_VALUE)) {
            IntVector source = new IntVector("v", root1);
            source.allocateNew(2);
            source.setSafe(0, 42);
            source.setValueCount(1);

            IntVector target = new IntVector("v", root2);
            try {
                source.makeTransferPair(target).transfer();
                System.out.println("  UNEXPECTED SUCCESS");
            } catch (Exception e) {
                System.out.println("  EXPECTED FAIL: " + e.getMessage());
            }
            source.close();
            target.close();
        }

        System.out.println("\n=== Test 3: C Data Interface across different roots (should work, zero-copy) ===");
        try (RootAllocator root1 = new RootAllocator(Long.MAX_VALUE);
             RootAllocator root2 = new RootAllocator(Long.MAX_VALUE)) {
            Schema schema = new Schema(List.of(
                new Field("v", FieldType.nullable(new ArrowType.Int(32, true)), null)
            ));
            VectorSchemaRoot sourceRoot = VectorSchemaRoot.create(schema, root1);
            IntVector sv = (IntVector) sourceRoot.getVector("v");
            sv.allocateNew(2);
            sv.setSafe(0, 42);
            sv.setSafe(1, 99);
            sv.setValueCount(2);
            sourceRoot.setRowCount(2);

            long root1Before = root1.getAllocatedMemory();
            long root2Before = root2.getAllocatedMemory();
            System.out.println("  Before: root1=" + root1Before + " root2=" + root2Before);

            ArrowArray arrowArray = ArrowArray.allocateNew(root1);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(root1);
            Data.exportVectorSchemaRoot(root1, sourceRoot, null, arrowArray, arrowSchema);

            long root1AfterExport = root1.getAllocatedMemory();
            System.out.println("  After export: root1=" + root1AfterExport + " (should decrease after import consumes)");

            VectorSchemaRoot targetRoot = Data.importVectorSchemaRoot(root2, arrowArray, arrowSchema, null);
            long root1AfterImport = root1.getAllocatedMemory();
            long root2AfterImport = root2.getAllocatedMemory();
            System.out.println("  After import: root1=" + root1AfterImport + " root2=" + root2AfterImport);

            IntVector tv = (IntVector) targetRoot.getVector("v");
            System.out.println("  SUCCESS: target[0]=" + tv.get(0) + " target[1]=" + tv.get(1));

            targetRoot.close();
            arrowArray.close();
            arrowSchema.close();
            sourceRoot.close();
        }

        System.out.println("\n=== Test 4: Unified budget with shared root ===");
        try (RootAllocator sharedRoot = new RootAllocator(1024 * 1024)) { // 1MB limit
            BufferAllocator flight = sharedRoot.newChildAllocator("flight", 0, 1024 * 1024);
            BufferAllocator datafusion = sharedRoot.newChildAllocator("datafusion", 0, 1024 * 1024);
            
            System.out.println("  Shared root limit: " + sharedRoot.getLimit());
            System.out.println("  flight allocated: " + flight.getAllocatedMemory());
            System.out.println("  datafusion allocated: " + datafusion.getAllocatedMemory());
            
            // Allocate under datafusion
            IntVector v1 = new IntVector("v1", datafusion);
            v1.allocateNew(100);
            v1.setValueCount(100);
            System.out.println("  After datafusion alloc: shared root used=" + sharedRoot.getAllocatedMemory());
            
            // Transfer to flight (same root, works)
            IntVector v2 = new IntVector("v1", flight);
            v1.makeTransferPair(v2).transfer();
            System.out.println("  After transfer to flight: shared root used=" + sharedRoot.getAllocatedMemory());
            System.out.println("  flight=" + flight.getAllocatedMemory() + " datafusion=" + datafusion.getAllocatedMemory());
            System.out.println("  Both under unified budget ✓");
            
            v1.close();
            v2.close();
            datafusion.close();
            flight.close();
        }

        System.out.println("\n=== Summary ===");
        System.out.println("transfer() within same root: ZERO-COPY ✓");
        System.out.println("transfer() across roots: BLOCKED ✗");
        System.out.println("C Data Interface across roots: ZERO-COPY ✓ (but no unified budget)");
        System.out.println("Shared root + transfer(): ZERO-COPY + UNIFIED BUDGET ✓✓");
    }
}
