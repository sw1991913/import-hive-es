package MyLog;


import com.proweb.common.timeop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;

/**
 * Created by Administrator on 8/23/2017.
 */
public class MyDfsLog implements Serializable {

    private static Configuration conf = new Configuration();

    public static void AddLog(String filename,String title,String content){
        conf.setBoolean("dfs.support.append", true);
        try {
            FileSystem fs = FileSystem.get(URI.create(filename),conf);
            Path path=new Path(filename);
            FSDataOutputStream fos;
            if (fs.exists(path)) {
                 fos = fs.append(new Path(filename));
            }
            else{
                 fos = fs.create(new Path(filename));
            }

            BufferedWriter outs = new BufferedWriter(new OutputStreamWriter(fos));
            if (null != fos) {
                String timestr= timeop.getDayFromTimedef(timeop.GetCurrentTime(), "yyyy-MM-dd HH:mm:ss");
                outs.write(String.format("%s\t%s\t%s\n",timestr,title,content));
                outs.close();
            }
            fos.close();
        }catch(Exception e){
        }
        conf.setBoolean("dfs.support.append", false);
    }

}
