package com.mobin.HDFS.FSUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * Created by Mobin on 2016/12/14.
 * 统计一个目录下的lzo文件的行数，每个lzo起一个task
 */
public class FSUtils {
    private static final Configuration conf = new Configuration();
    private static final FileSystem fs = null;

    public static void main(String[] args) throws IOException {
        String file = "E:\\DATA\\PUBLIC\\NOCE\\AGG\\AGG_EVT_LTE_DPI_NEW\\hour=2016102011\\m_p_0.txt.lzo";
        int lineCount = 0;
         Configuration conf = new Configuration();
         FileSystem fs = FileSystem.get(conf);
         try(BufferedReadIterable br = new BufferedReadIterable(fs,file)){
             for(String line : br){

             }
         }

    }

    public static BufferedReadIterable createBuferedReadIterable(FileSystem fs, String file) throws IOException {
        return  new BufferedReadIterable(fs,file);
    }

    public static class BufferedReadIterable implements Iterable<String>,Closeable{
        private final String file;
        private final long size;
        private BufferedReader br;


        public BufferedReadIterable(FileSystem fs, String file) throws IOException {
            this.file = file;
            Path path = new Path(file);
            this.size = fs.getFileStatus(path).getLen();

            CompressionCodecFactory factory = new CompressionCodecFactory(fs.getConf());
            //HDFS根据文件的后缀来确定使用的是哪种压缩算法
            CompressionCodec codec = factory.getCodec(path);

            FSDataInputStream inputStream = fs.open(path,8192);
            if(codec == null){
                br = new BufferedReader(new InputStreamReader(inputStream));
            }else{
                //先解压再读取
                CompressionInputStream comIn  = codec.createInputStream(inputStream);
                br = new BufferedReader(new InputStreamReader(comIn));
            }
        }

        @Override
        public void close() throws IOException {
            br.close();
        }

        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>() {
                private String line;
                @Override
                public boolean hasNext() {
                    try {
                        line = br.readLine();
                    } catch (IOException e) {
                       line = null;
                    }
                    return line != null;
                }

                @Override
                public String next() {
                    return line;
                }

                @Override
                public void remove() {
                      throw new UnsupportedOperationException("remove");
                }
            };
        }
    }
}
