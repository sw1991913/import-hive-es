import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class hdfs_filecopy{
	
	/*
	 * FileSystem dfs,
		String soupath,
		String destpath,
		boolean bappend,    true 尾部增加 false 新建
		String process_suffix  null  复制文件过程标记 默认为 .process
	 */
	public static boolean copyfile_dfs_dfs(FileSystem dfs,
									String soupath,
									String destpath,
									boolean bappend,
									String process_suffix){
		try {
			System.out.println("==>copyfile_dfs_dfs: souptah:"+soupath+" destpath:"+destpath+" bappend:"+bappend);
			
			String suffix=process_suffix;
			if(suffix==null)suffix=".process";
			String destpath_process=destpath+suffix;
			
			if(bappend){
				destpath_process=destpath;
				suffix=null;
			}
		
			StringBuilder  msg=new StringBuilder(); 
			FSDataInputStream in = dfs.open(new Path(soupath));
			FSDataOutputStream out;
			msg.append("copyfile :");
			if(bappend){
				Path path=new Path(destpath_process);
				if(dfs.exists(path)){
					msg.append(" dfs.append");
					out = dfs.append(path);
				}else{
					msg.append(" dfs.create");
					out = dfs.create(path);
				}
			}else{
				msg.append(" dfs.create2");
				out = dfs.create(new Path(destpath_process));
			}
			IOUtils.copyBytes(in, out, 4096, true);
			
			if(!destpath_process.equals(destpath)){
				Path destpath_process_path=new Path(destpath_process);
				Path destpath_path=new Path(destpath);
				dfs.rename(destpath_process_path, destpath_path);
			}
			
			
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}  
		return true;
	}
	
	/*
	 * FileSystem dfs,
		String soupath,
		String destpath,
		String process_suffix  null  复制文件过程标记 默认为 .process
	 */
	public static boolean copyfile_local_dfs(FileSystem dfs,
									String soupath,
									String destpath,
									String process_suffix){
		try {
			String suffix=process_suffix;
			if(suffix==null)suffix=".process";
			
			String destpath_process=destpath+suffix;
			Path soupath_path=new Path(soupath);
			Path destpath_process_path=new Path(destpath_process);
			dfs.copyFromLocalFile(soupath_path, destpath_process_path);
		
			Path destpath_path=new Path(destpath);
			dfs.rename(destpath_process_path, destpath_path);
			System.out.println("copyfile_local_dfs: souptah:"+soupath+" destpath:"+destpath);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}  
		return true;
	}
	
	/*
	 * FileSystem dfs,
		String soupath,
		String destpath,
		String process_suffix  null  复制文件过程标记 默认为 .process
	 */
	public static boolean copyfile_dfs_local(FileSystem dfs,
									String soupath,
									String destpath,
									String process_suffix){
		try {
			String suffix=process_suffix;
			if(suffix==null)suffix=".process";
			
			String destpath_process=destpath+suffix;
			Path soupath_path=new Path(soupath);
			Path destpath_process_path=new Path(destpath_process);
			dfs.copyToLocalFile(soupath_path, destpath_process_path);
		
			File  file=new File(destpath_process);
			File dest=new File(destpath);
			file.renameTo(dest);
			System.out.println("copyfile_dfs_local: souptah:"+soupath+" destpath:"+destpath);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}  
		return true;
	}
	
	
	/*
		String srcFileName,
		String destFileName,
		String process_suffix  null  复制文件过程标记 默认为 .process
	 */
	public static boolean copyfile_local_local(String srcFileName, String destFileName, String process_suffix) {  
        File srcFile = new File(srcFileName);  
  
        String suffix=process_suffix;
		if(suffix==null)suffix=".process";
		String destpath_process=destFileName+suffix;
		
        if (!srcFile.exists()) {  
            return false;  
        } else if (!srcFile.isFile()) {  
              
            return false;  
        }   
        File destFile_process = new File(destpath_process);  
        if (destFile_process.exists()) {        
             new File(destpath_process).delete();  
        } else {   
            if (!destFile_process.getParentFile().exists()) {    
                if (!destFile_process.getParentFile().mkdirs()) {    
                    return false;  
                }  
            }  
        }  
        int byteread = 0; // 锟斤拷取锟斤拷锟街斤拷锟斤拷  
        InputStream in = null;  
        OutputStream out = null;  
  
        try {  
            in = new FileInputStream(srcFile);  
            out = new FileOutputStream(destFile_process);  
            byte[] buffer = new byte[1024];  
  
            while ((byteread = in.read(buffer)) != -1) {  
                out.write(buffer, 0, byteread);  
            }  
            File destFile=new File(destFileName);
            destFile_process.renameTo(destFile);
            return true;  
        } catch (FileNotFoundException e) {  
            return false;  
        } catch (IOException e) {  
            return false;  
        } finally {  
            try {  
                if (out != null)  
                    out.close();  
                if (in != null)  
                    in.close();  
            } catch (IOException e) {  
                e.printStackTrace();  
            }
        }  
        
    }  
}
