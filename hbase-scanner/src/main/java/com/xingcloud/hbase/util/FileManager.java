package com.xingcloud.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-6
 * Time: 下午3:04
 * To change this template use File | Settings | File Templates.
 */
public class FileManager {
    private static Logger LOG = LoggerFactory.getLogger(FileManager.class);

    public static List<Path> listDirPath(String dir) throws IOException {
        LOG.info("List " + dir);
        List<Path> paths = new ArrayList<Path>();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(dir);
        if (fs.isFile(dirPath)) {
            LOG.warn(dir + " is not a dir!");
            return paths;
        } else {
            if (fs.exists(dirPath)) {
                FileStatus[] fileList = fs.listStatus(dirPath);
                if (fileList == null) {
                    return paths;
                }
                for (FileStatus file : fileList) {
                    paths.add(file.getPath());
                }
            }
        }
        return paths;
    }

}
