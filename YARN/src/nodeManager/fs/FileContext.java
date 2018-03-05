package nodeManager.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

public class FileContext {
	/**
	 * 创建文件
	 * @param path
	 * @return 新创建的文件
	 */
	public File create(String path){
		File file = new File(path);
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return file;
	}

	/**
	 * 创建目录
	 * @param path
	 * @return 新创建的目录
	 */
	public File mkdir(String path){
		File file = new File(path);
		file.mkdirs();
		return file;
	}
	/**
	 * 删除文件或目录
	 * @param path
	 * @return
	 */
	public boolean delete(String path){
		boolean flag=false;
		File file = new File(path);
		flag=file.delete();
		return flag;
	}
	/**
	 * 文件复制
	 * @param src
	 * @param dest
	 * @return
	 */
	public long copyFile(String src,String dest){
		long count=-1;
		FileChannel inputChannel = null;    
		FileChannel outputChannel = null;
		try {
			File destFile=new File(dest);
			if(!destFile.isFile())
				destFile.createNewFile();
			inputChannel = new FileInputStream(new File(src)).getChannel();
			outputChannel = new FileOutputStream(destFile).getChannel();
			count=outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				inputChannel.close();
				outputChannel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return count;
	}
	/** 
	 * 复制一个目录及其子目录、文件到另外一个目录 
	 * @param src 
	 * @param dest 
	 * @throws IOException 
	 */  
	public void copy(String srcPath,String destPath)  {  
		try{
			File src=new File(srcPath);
			File dest=new File(destPath);
			if (src.isDirectory()) {  
				if (!dest.exists()) {  
					dest.mkdir();  
				}  
				String files[] = src.list();  
				for (String file : files) {  
					// 递归复制  
					copy(srcPath+"/"+file, destPath+"/"+file);  
				}  
			} else if (src.isFile()){  
				copyFile(srcPath,destPath);
			}  else{
				System.err.println("input srcPath is neither a file nor a directory");
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}  
}