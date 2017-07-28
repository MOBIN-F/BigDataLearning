package com.mobin.HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * Created by Mobin on 2016/12/19.
 */
public class HDFSCompressionCodec {
    private static  final Configuration conf = new Configuration();
    private static FileSystem fs = null;
    //压缩
    public void coder(String path) throws IOException, ClassNotFoundException {
        //获取文件输入流
        File dir = new File(path);
        System.out.println(dir.isDirectory());
        conf.set("mapred.output.compress", "true");
        conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
        fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path("E:\\DATA\\PUBLIC\\NOCE\\school5.lzo"));
        Class<?> codecClass = Class.forName("com.hadoop.compression.lzo.LzopCodec");
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        //将压缩数据写入到school.gz中
        //创建CompressionInputStream来对文件进行压缩
        CompressionOutputStream codecout = codec.createOutputStream(out);
        for(File file:dir.listFiles() ) {
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
                try{
                    //最后个参数为true时同时关闭输出流和输入流
                    IOUtils.copyBytes(in, codecout, 4096, false);
                }finally {
                    IOUtils.closeStream(in);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        out.flush();
        out.close();
    }

    //解压
    public void decoder() throws IOException {
        fs = FileSystem.get(conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(fs.getConf());
        //根据文件的后缀名来确定使用的是哪种压缩算法
        Path path = new Path("E:\\DATA\\PUBLIC\\NOCE\\school.gz");
        CompressionCodec codec = factory.getCodec(path);
        try(FSDataInputStream inputStream = fs.open(path,8096)){
            //创建CompressionInputStream来对文件进行解压
            CompressionInputStream comInputStream = codec.createInputStream(inputStream);
            //将解压后的文件写到school.txt
            FSDataOutputStream out = fs.create(new Path("E:\\DATA\\PUBLIC\\NOCE\\school5.txt"));
            IOUtils.copyBytes(comInputStream,out,4096,false);
            comInputStream.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String path = "E:\\DATA\\PUBLIC\\NOCE\\sch";
        HDFSCompressionCodec codec = new HDFSCompressionCodec();
        codec.coder(path);
       codec.decoder();
    }
}
