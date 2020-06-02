package org.apache.orc.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class IntWriterDemo {
    public static void main(String[] args) throws IOException {
        main(new Configuration(), args);
    }

    public static void main(Configuration conf, String[] args) throws IOException {
        LocalFileSystem local = FileSystem.getLocal(conf);
        local.delete(new Path("int-file.orc"), true);
        TypeDescription schema =
                TypeDescription.fromString("struct<x:int>");
        Writer writer = OrcFile.createWriter(new Path("int-file.orc"),
                OrcFile.writerOptions(conf)
                        .setSchema(schema));
        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector x = (LongColumnVector) batch.cols[0];
        int[] datas = {10000, 10000, 10000, 10000, 10000};
        for (int data : datas) {
            int row = batch.size++;
            x.vector[row] = data;
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
        }
        writer.close();
    }
}
