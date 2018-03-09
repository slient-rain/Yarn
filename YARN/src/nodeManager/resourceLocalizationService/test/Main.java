package nodeManager.resourceLocalizationService.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import util.PropertiesFile;

import nodeManager.fs.FileContext;
import nodeManager.fs.Path;
import nodeManager.resourceLocalizationService.FSDownload;
import nodeManager.test.NodemanagerMian;

public class Main {
	public static void main(String[] args) {
		int spos="abcc?file=5".indexOf("file");
		System.out.println("abcc?file=5".substring(spos+5, "abcc?file=5".length()));
		PropertiesFile pf=new PropertiesFile("config.properties");
		FileContext lfs=new FileContext();
		String LOCAL_DIR=pf.get("local_dir");
		Path path=new Path(LOCAL_DIR+(Path.SEPARATOR)+"mkdirTest1");
//		System.out.println("ResourceLocalizationService.addResource():"+request.toString());
		System.out.println("ResourceLocalizationService.addResource().mkdir.path:"+path.toString());
		lfs.mkdir(path.toString());
		
//		//用HttpClient发送请求，分为五步
//		//第一步：创建HttpClient对象
//		HttpClient httpCient = new DefaultHttpClient();
//		//第二步：创建代表请求的对象,参数是访问的服务器地              
//		HttpGet httpGet = new HttpGet("http://"+"192.168.1.59"+":"+"8080"+"/"+"AppStore"+"/"+"Download");
//		try {
//			//第三步：执行请求，获取服务器发还的相应对象
//			HttpResponse httpResponse = httpCient.execute(httpGet);
//			//第四步：检查相应的状态是否正常：检查状态码的值是200表示正常
//			if (httpResponse.getStatusLine().getStatusCode() == 200) {
//				//第五步：从相应对象当中取出数据，放到entity当中
//				HttpEntity entity = httpResponse.getEntity();
//				InputStream in=entity.getContent();
//				// 手动写的
//				OutputStream out = new FileOutputStream(new File("e://",
//						"cgroup.jar"));
//				int length = 0;
//				byte[] buf = new byte[1024];
//
//				// System.out.println("获取上传文件的总共的容量：" + item.getSize());
//
//				// in.read(buf) 每次读到的数据存放在 buf 数组中
//				while ((length = in.read(buf)) != -1) {
//					// 在 buf 数组中 取出数据 写到 （输出流）磁盘上
//					out.write(buf, 0, length);
//				}
//
//			}  
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
}
