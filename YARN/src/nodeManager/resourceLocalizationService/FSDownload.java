/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nodeManager.resourceLocalizationService;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nodeManager.fs.Path;
import protocol.protocolWritable.ApplicationSubmissionContext.LocalResource;



/**
 * Download a single URL to the local disk.
 *
 */
public class FSDownload implements Callable<Path> {

	
	private LocalResource resource;
	
	private Path destDirPath;
	private static final Logger LOG = LoggerFactory
			.getLogger(FSDownload.class);

	public FSDownload(
			//			FileContext files,
			//			UserGroupInformation ugi, 
			//			Configuration conf,
			Path destDirPath,
			LocalResource resource) {
		//		this.conf = conf;
		this.destDirPath = destDirPath;
		//		this.files = files;
		//		this.userUgi = ugi;
		this.resource = resource;
	}

	@Override
	public Path call() throws Exception {
		String host=resource.getResource().getHost();
		int port=resource.getResource().getPort();
		String file=resource.getResource().getFile();
		String url="http://"+host+":"+port+"/"+file;
		LOG.debug("FSDownload.call():request url:"+url);
		//用HttpClient发送请求，分为五步
		//第一步：创建HttpClient对象
		HttpClient httpCient = new DefaultHttpClient();
		//第二步：创建代表请求的对象,参数是访问的服务器地              
		HttpGet httpGet = new HttpGet(url);
		try {
			//第三步：执行请求，获取服务器发还的相应对象
			HttpResponse httpResponse = httpCient.execute(httpGet);
			//第四步：检查相应的状态是否正常：检查状态码的值是200表示正常
			if (httpResponse.getStatusLine().getStatusCode() == 200) {
				//第五步：从相应对象当中取出数据，放到entity当中
				HttpEntity entity = httpResponse.getEntity();
				InputStream in=entity.getContent();
				// 手动写的
				String filePath=resource.getResource().getFile();
				int spos=filePath.indexOf("file");
				String fileName=filePath.substring(spos+5,filePath.length());
				OutputStream out = new FileOutputStream(new File(destDirPath.toString(),fileName));
				int length = 0;
				byte[] buf = new byte[1024];

				// System.out.println("获取上传文件的总共的容量：" + item.getSize());

				// in.read(buf) 每次读到的数据存放在 buf 数组中
				while ((length = in.read(buf)) != -1) {
					// 在 buf 数组中 取出数据 写到 （输出流）磁盘上
					out.write(buf, 0, length);
				}

			}  
			LOG.debug("FSDownload.call():下载完成");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error(e.toString());
		}
		return destDirPath;
	}

}
