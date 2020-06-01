package org.apache.orc.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;

public class StringReaderDemo {

    public static void main(String[] args) throws IOException {
        main(new Configuration(), args);
    }

    public static void main(Configuration conf, String[] args) throws IOException {
        // Get the information from the file footer
        Reader reader = OrcFile.createReader(new Path("string-file.orc"),
                OrcFile.readerOptions(conf));
        System.out.println("File schema: " + reader.getSchema());
        System.out.println("Row count: " + reader.getNumberOfRows());

        // Pick the schema we want to read using schema evolution
        TypeDescription readSchema =
                TypeDescription.fromString("struct<x:string>");
        // Read the row data
        VectorizedRowBatch batch = readSchema.createRowBatch();
        RecordReader rowIterator = reader.rows(reader.options()
                .schema(readSchema));
        BytesColumnVector x = (BytesColumnVector) batch.cols[0];
        while (rowIterator.nextBatch(batch)) {
            for (int row = 0; row < batch.size; ++row) {
                System.out.println("x: " + x.toString(row));
            }
        }
        rowIterator.close();
    }
}
