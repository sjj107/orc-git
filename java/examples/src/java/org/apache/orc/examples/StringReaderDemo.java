package org.apache.orc.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
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
                TypeDescription.fromString("struct<x:string,y:int>");
        // Read the row data
        VectorizedRowBatch batch = readSchema.createRowBatch();
        RecordReader rowIterator = reader.rows(reader.options()
//                .searchArgument(SearchArgumentFactory.newBuilder().equals("x", PredicateLeaf.Type.STRING, "ab").build(), new String[]{"x"})
//                .searchArgument(SearchArgumentFactory.newBuilder().in("x", PredicateLeaf.Type.STRING,new String[]{"ab","b"}).build(), new String[]{"x"})
//                .searchArgument(SearchArgumentFactory.newBuilder().in("x", PredicateLeaf.Type.STRING,new String[]{"c","ab"}).build(), new String[]{"x"})
//                .searchArgument(SearchArgumentFactory.newBuilder().startOr().equals("x", PredicateLeaf.Type.STRING, "ab").equals("x", PredicateLeaf.Type.STRING, "c").end().build(), new String[]{"x"})
//                .searchArgument(SearchArgumentFactory.newBuilder().startAnd().equals("x", PredicateLeaf.Type.STRING, "abc").equals("y", PredicateLeaf.Type.LONG, -1L).end().build(), new String[]{"x","y"})
                .searchArgument(SearchArgumentFactory.newBuilder().startAnd().equals("x", PredicateLeaf.Type.STRING, "d").between("y", PredicateLeaf.Type.LONG, -9L,-1L).end().build(), new String[]{"x","y"})
//                .searchArgument(SearchArgumentFactory.newBuilder().equals("y", PredicateLeaf.Type.LONG, -2L).build(), new String[]{"y"})
                .schema(readSchema));
        BytesColumnVector x = (BytesColumnVector) batch.cols[0];
        int num = 0;
        while (rowIterator.nextBatch(batch)) {
            for (int row = 0; row < batch.size; ++row) {
                System.out.println("x: " + x.toString(row));
                num++;
            }
        }
        System.out.println("num: " + num);
        rowIterator.close();
    }
}
