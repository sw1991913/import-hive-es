package MergeFile; /**
 * Created by probdViewSonic on 2017/2/28.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

public class Merge {

//    protected static long FILE_LIMIT_SIZE = 256 * 1024 * 1024;
//    protected static long FILE_LIMIT_TIME = 3 * 60 * 60 * 1000;
    private long limitSize;
    private long limitTime;
    private String[] libDirArray = null;
    private String tmpDir = null;
    protected static Random random = new Random();

    protected Configuration conf = null;
    protected FileSystem fs=null;

    public Merge(String libtDirs, String tmpDir, long limitSize, long limitTime) {

        libDirArray = libtDirs.split(",");
        this.tmpDir = tmpDir;
        this.limitSize=limitSize;
        this.limitTime=limitTime;
        conf = new Configuration();
        conf.addDefaultResource("hdfs-site.xml");

        try {
            fs = FileSystem.get(URI.create(libDirArray[0]), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void start() throws Exception {

        Path tmpPath = new Path(tmpDir);

        work(tmpPath);
    }
    /*
    * 1.根据用户定义的参数设置本地目录和HDFS的目标文件
    *
    * 2.创建一个输出流写入到HDFS文件
    *
    * 3.遍历本地目录中的每个文件，打开文件，并读取文件内容，将文件的内容写到HDFS文件中。
    */
    private void work1(Path path) throws Exception {

        int dirIndex = random.nextInt(libDirArray.length);
        String libDir = libDirArray[dirIndex];

        Path inPath, outPath = null;
        FSDataOutputStream out = null;
        BufferedInputStream in;

        LocatedFileStatus locatedFile = null;
        RemoteIterator<LocatedFileStatus> fileIt = fs.listFiles(path, true);

        while (fileIt.hasNext()) {
            locatedFile = fileIt.next();

            if (locatedFile.isDirectory()) {

                work(locatedFile.getPath());
            } else if (locatedFile.isFile()) {

                if (out == null) {

                    outPath = locatedFile.getPath();
                    out = fs.append(outPath);
                } else {

                    inPath = locatedFile.getPath();
                    FSDataInputStream inputStream = fs.open(inPath);
                    in = new BufferedInputStream(inputStream);

                    IOUtils.copyBytes(in, out, 4096, false);
                    out.flush();

                    in.close();
                    inputStream.close();

                    fs.delete(inPath, false);
                }

                FileStatus fileStatus = fs.getFileStatus(outPath);
                if (fileStatus.getLen() >= limitSize
                    || System.currentTimeMillis() - fileStatus.getModificationTime() >= limitTime) {

                    out.close();
                    out = null;

                    StringBuffer parentPath = new StringBuffer(outPath.getParent().toString());
                    int index = parentPath.indexOf(tmpDir);
                    parentPath.replace(index, index + tmpDir.length(), libDir);

                    String fileName = outPath.getName();
                    int lastIndex = fileName.lastIndexOf(".");

                    Path newPath = new Path(parentPath.toString() + "/" + fileName);
                    int dupCount = 0;
                    while (fs.exists(newPath)) {

                        dupCount++;
                        newPath = new Path(parentPath.toString() + "/"
                                + (lastIndex < 0 ? fileName : fileName.substring(0, lastIndex))
                                + dupCount
                                + (lastIndex < 0 ? "" : fileName.substring(lastIndex)));
                    }
                    fs.rename(outPath, newPath);
                    System.out.println(newPath.toString());
                    outPath = null;
                }
            }
        }

        if (out != null) out.close();

    }



    private void work(Path path) throws Exception {

        int dirIndex = random.nextInt(libDirArray.length);
        String libDir = libDirArray[dirIndex];

        Path inPath, outPath = null;
        FSDataOutputStream out = null;
        BufferedInputStream in;

        FileStatus[] fileArray = fs.listStatus(path);
        for (FileStatus fileStatus : fileArray) {

            if (fileStatus.isDirectory()) {

                work(fileStatus.getPath());
            } else if (fileStatus.isFile()) {

                if (out == null) {

                    outPath = fileStatus.getPath();
                    out = fs.append(outPath);
                } else {

                    inPath = fileStatus.getPath();
                    FSDataInputStream inputStream = fs.open(inPath);
                    in = new BufferedInputStream(inputStream);

                    IOUtils.copyBytes(in, out, 4096, false);
                    out.flush();

                    in.close();
                    inputStream.close();

                    fs.delete(inPath, false);
                }

                FileStatus newStatus = fs.getFileStatus(outPath);
                if (newStatus.getLen() >= limitSize
                        || System.currentTimeMillis() - newStatus.getModificationTime() >= limitTime) {

                    out.close();
                    out = null;

                    StringBuffer parentPath = new StringBuffer(outPath.getParent().toString());
                    int index = parentPath.indexOf(tmpDir);
                    parentPath.replace(index, index + tmpDir.length(), libDir);

                    String fileName = outPath.getName();
                    int lastIndex = fileName.lastIndexOf(".");

                    Path newPath = new Path(parentPath.toString() + "/" + fileName);
                    int dupCount = 0;
                    while (fs.exists(newPath)) {

                        dupCount++;
                        newPath = new Path(parentPath.toString() + "/"
                                + (lastIndex < 0 ? fileName : fileName.substring(0, lastIndex))
                                + dupCount
                                + (lastIndex < 0 ? "" : fileName.substring(lastIndex)));
                    }
                    fs.rename(outPath, newPath);
                    System.out.println(newPath.toString());
                    outPath = null;
                }
            }
        }

        if (out != null) out.close();

    }
    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage:nt"+ Merge.class.getName()
                    +"[LocalPath] [HDFSPath]");
            System.exit(1);
        }
        new Merge(args[0], args[1],Long.valueOf(args[2]),Long.valueOf(args[3])).start();
    }
}
