package org.apache.orc.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.*;

import java.io.IOException;

/**
 * bloomFilter示例
 */
public class BoolmFilterReadDemo {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        // Get the information from the file footer
        Reader reader = OrcFile.createReader(new Path("bloom-file.orc"),
                OrcFile.readerOptions(conf));
        System.out.println("File schema: " + reader.getSchema());
        System.out.println("Row count: " + reader.getNumberOfRows());

        // Pick the schema we want to read using schema evolution
        TypeDescription readSchema =
                TypeDescription.fromString("struct<x:int>");
        // Read the row data
        VectorizedRowBatch batch = readSchema.createRowBatch();
        RecordReader rowIterator = reader.rows(reader.options()
                .searchArgument(SearchArgumentFactory.newBuilder().equals("x", PredicateLeaf.Type.LONG, 99L).build(), new String[]{"x"})
                .schema(readSchema));
        LongColumnVector x = (LongColumnVector) batch.cols[0];
        int num = 0;
        while (rowIterator.nextBatch(batch)) {
            for (int row = 0; row < batch.size; ++row) {
                System.out.println("x: " + x.vector[row]);
                num++;
            }
        }
        System.out.println("num: " + num);
        rowIterator.close();
    }
}
