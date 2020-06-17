package org.apache.orc.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 一个字段，字段类型为string
 */
public class StringWriterDemo {

    public static void main(String[] args) throws IOException {
        main(new Configuration(), args);
    }

    public static void main(Configuration conf, String[] args) throws IOException {
        LocalFileSystem local = FileSystem.getLocal(conf);
        local.delete(new Path("string-file.orc"), true);
        TypeDescription schema =
                TypeDescription.fromString("struct<x:string>");
        Writer writer = OrcFile.createWriter(new Path("string-file.orc"),
                OrcFile.writerOptions(conf)
                        .setSchema(schema));
        VectorizedRowBatch batch = schema.createRowBatch();
        BytesColumnVector x = (BytesColumnVector) batch.cols[0];
        //这种情况重复率不高，使用DIRECT_V2编码
//        String[] datas = {"01", "234"};
//        //这种情况重复率高，使用字典编码
//        String[] datas = {"ab", "ab", "ab", "cd"};
//        for (String data : datas) {
//            int row = batch.size++;
//            byte[] buffer = data.getBytes(StandardCharsets.UTF_8);
//            x.setRef(row, buffer, 0, buffer.length);
//            // If the batch is full, write it out and start over.
//            if (batch.size == batch.getMaxSize()) {
//                writer.addRowBatch(batch);
//                batch.reset();
//            }
//        }


        //以下测试是为了StringReaderDemo使用searchArgument，稀疏索引：只能按照rowgroup（10000条数据）来过滤
        int row = batch.size++;
        byte[] buffer = "ab".getBytes(StandardCharsets.UTF_8);
        x.setRef(row, buffer, 0, buffer.length);
        row = batch.size++;
        buffer = "b".getBytes(StandardCharsets.UTF_8);
        x.setRef(row, buffer, 0, buffer.length);
        for (int i = 0; i < 40000; i++) {
            row = batch.size++;
            buffer = "a".getBytes(StandardCharsets.UTF_8);
            x.setRef(row, buffer, 0, buffer.length);
            // If the batch is full, write it out and start over.
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        //以下测试是为了StringReaderDemo使用searchArgument，稀疏索引：只能按照rowgroup（10000条数据）来过滤

        if (batch.size != 0) {
            writer.addRowBatch(batch);
        }
        writer.close();
    }
}
