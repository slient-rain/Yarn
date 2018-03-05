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

import nodeManager.fs.Path;

import protocol.protocolWritable.ApplicationSubmissionContext.LocalResource;



/**
 * Download a single URL to the local disk.
 *
 */
public class FSDownload implements Callable<Path> {

	//  private static final Log LOG = LogFactory.getLog(FSDownload.class);
	//
	//  private FileContext files;
	//  private final UserGroupInformation userUgi;
	//  private Configuration conf;
	private LocalResource resource;
	//  
	//  /** The local FS dir path under which this resource is to be localized to */
	private Path destDirPath;
	//
	//  private static final FsPermission cachePerms = new FsPermission(
	//      (short) 0755);
	//  static final FsPermission PUBLIC_FILE_PERMS = new FsPermission((short) 0555);
	//  static final FsPermission PRIVATE_FILE_PERMS = new FsPermission(
	//      (short) 0500);
	//  static final FsPermission PUBLIC_DIR_PERMS = new FsPermission((short) 0755);
	//  static final FsPermission PRIVATE_DIR_PERMS = new FsPermission((short) 0700);
	//

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

	//	LocalResource getResource() {
	//		return resource;
	//	}
	//
	//	private void createDir(Path path, FsPermission perm) throws IOException {
	//		files.mkdir(path, perm, false);
	//		if (!perm.equals(files.getUMask().applyUMask(perm))) {
	//			files.setPermission(path, perm);
	//		}
	//	}
	//
	//	/**
	//	 * Returns a boolean to denote whether a cache file is visible to all(public)
	//	 * or not
	//	 * @param conf
	//	 * @param uri
	//	 * @return true if the path in the uri is visible to all, false otherwise
	//	 * @throws IOException
	//	 */
	//	private static boolean isPublic(FileSystem fs, Path current) throws IOException {
	//		current = fs.makeQualified(current);
	//		//the leaf level file should be readable by others
	//		if (!checkPublicPermsForAll(fs, current, FsAction.READ_EXECUTE, FsAction.READ)) {
	//			return false;
	//		}
	//		return ancestorsHaveExecutePermissions(fs, current.getParent());
	//	}
	//
	//	private static boolean checkPublicPermsForAll(FileSystem fs, Path current, 
	//			FsAction dir, FsAction file) 
	//					throws IOException {
	//		return checkPublicPermsForAll(fs, fs.getFileStatus(current), dir, file);
	//	}
	//
	//	private static boolean checkPublicPermsForAll(FileSystem fs, 
	//			FileStatus status, FsAction dir, FsAction file) 
	//					throws IOException {
	//		FsPermission perms = status.getPermission();
	//		FsAction otherAction = perms.getOtherAction();
	//		if (status.isDirectory()) {
	//			if (!otherAction.implies(dir)) {
	//				return false;
	//			}
	//
	//			for (FileStatus child : fs.listStatus(status.getPath())) {
	//				if(!checkPublicPermsForAll(fs, child, dir, file)) {
	//					return false;
	//				}
	//			}
	//			return true;
	//		}
	//		return (otherAction.implies(file));
	//	}
	//
	//	/**
	//	 * Returns true if all ancestors of the specified path have the 'execute'
	//	 * permission set for all users (i.e. that other users can traverse
	//	 * the directory heirarchy to the given path)
	//	 */
	//	private static boolean ancestorsHaveExecutePermissions(FileSystem fs, Path path)
	//			throws IOException {
	//		Path current = path;
	//		while (current != null) {
	//			//the subdirs in the path should have execute permissions for others
	//			if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE)) {
	//				return false;
	//			}
	//			current = current.getParent();
	//		}
	//		return true;
	//	}
	//
	//	/**
	//	 * Checks for a given path whether the Other permissions on it 
	//	 * imply the permission in the passed FsAction
	//	 * @param fs
	//	 * @param path
	//	 * @param action
	//	 * @return true if the path in the uri is visible to all, false otherwise
	//	 * @throws IOException
	//	 */
	//	private static boolean checkPermissionOfOther(FileSystem fs, Path path,
	//			FsAction action) throws IOException {
	//		FileStatus status = fs.getFileStatus(path);
	//		FsPermission perms = status.getPermission();
	//		FsAction otherAction = perms.getOtherAction();
	//		return otherAction.implies(action);
	//	}
	//
	//
	//	private Path copy(Path sCopy, Path dstdir) throws IOException {
	//		FileSystem sourceFs = sCopy.getFileSystem(conf);
	//		Path dCopy = new Path(dstdir, "tmp_"+sCopy.getName());
	//		FileStatus sStat = sourceFs.getFileStatus(sCopy);
	//		if (sStat.getModificationTime() != resource.getTimestamp()) {
	//			throw new IOException("Resource " + sCopy +
	//					" changed on src filesystem (expected " + resource.getTimestamp() +
	//					", was " + sStat.getModificationTime());
	//		}
	//		if (resource.getVisibility() == LocalResourceVisibility.PUBLIC) {
	//			if (!isPublic(sourceFs, sCopy)) {
	//				throw new IOException("Resource " + sCopy +
	//						" is not publicly accessable and as such cannot be part of the" +
	//						" public cache.");
	//			}
	//		}
	//
	//		sourceFs.copyToLocalFile(sCopy, dCopy);
	//		return dCopy;
	//	}
	//
	//	private long unpack(File localrsrc, File dst, Pattern pattern) throws IOException {
	//		switch (resource.getType()) {
	//		case ARCHIVE: {
	//			String lowerDst = dst.getName().toLowerCase();
	//			if (lowerDst.endsWith(".jar")) {
	//				RunJar.unJar(localrsrc, dst);
	//			} else if (lowerDst.endsWith(".zip")) {
	//				FileUtil.unZip(localrsrc, dst);
	//			} else if (lowerDst.endsWith(".tar.gz") ||
	//					lowerDst.endsWith(".tgz") ||
	//					lowerDst.endsWith(".tar")) {
	//				FileUtil.unTar(localrsrc, dst);
	//			} else {
	//				LOG.warn("Cannot unpack " + localrsrc);
	//				if (!localrsrc.renameTo(dst)) {
	//					throw new IOException("Unable to rename file: [" + localrsrc
	//							+ "] to [" + dst + "]");
	//				}
	//			}
	//		}
	//		break;
	//		case PATTERN: {
	//			String lowerDst = dst.getName().toLowerCase();
	//			if (lowerDst.endsWith(".jar")) {
	//				RunJar.unJar(localrsrc, dst, pattern);
	//				File newDst = new File(dst, dst.getName());
	//				if (!dst.exists() && !dst.mkdir()) {
	//					throw new IOException("Unable to create directory: [" + dst + "]");
	//				}
	//				if (!localrsrc.renameTo(newDst)) {
	//					throw new IOException("Unable to rename file: [" + localrsrc
	//							+ "] to [" + newDst + "]");
	//				}
	//			} else if (lowerDst.endsWith(".zip")) {
	//				LOG.warn("Treating [" + localrsrc + "] as an archive even though it " +
	//						"was specified as PATTERN");
	//				FileUtil.unZip(localrsrc, dst);
	//			} else if (lowerDst.endsWith(".tar.gz") ||
	//					lowerDst.endsWith(".tgz") ||
	//					lowerDst.endsWith(".tar")) {
	//				LOG.warn("Treating [" + localrsrc + "] as an archive even though it " +
	//						"was specified as PATTERN");
	//				FileUtil.unTar(localrsrc, dst);
	//			} else {
	//				LOG.warn("Cannot unpack " + localrsrc);
	//				if (!localrsrc.renameTo(dst)) {
	//					throw new IOException("Unable to rename file: [" + localrsrc
	//							+ "] to [" + dst + "]");
	//				}
	//			}
	//		}
	//		break;
	//		case FILE:
	//		default:
	//			if (!localrsrc.renameTo(dst)) {
	//				throw new IOException("Unable to rename file: [" + localrsrc
	//						+ "] to [" + dst + "]");
	//			}
	//			break;
	//		}
	//		if(localrsrc.isFile()){
	//			try {
	//				files.delete(new Path(localrsrc.toString()), false);
	//			} catch (IOException ignore) {
	//			}
	//		}
	//		return 0;
	//		// TODO Should calculate here before returning
	//		//return FileUtil.getDU(destDir);
	//	}

	@Override
	public Path call() throws Exception {
		String host=resource.getResource().getHost();
		int port=resource.getResource().getPort();
		String file=resource.getResource().getFile();
		String url="http://"+host+":"+port+"/"+file;
		System.out.println("FSDownload.call():request url:"+url);
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
			System.out.println("FSDownload.call():下载完成");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}




		//		final Path sCopy;
		//		try {
		//			sCopy = ConverterUtils.getPathFromYarnURL(resource.getResource());
		//		} catch (URISyntaxException e) {
		//			throw new IOException("Invalid resource", e);
		//		}
		//		createDir(destDirPath, cachePerms);
		//		final Path dst_work = new Path(destDirPath + "_tmp");
		//		createDir(dst_work, cachePerms);
		//		Path dFinal = files.makeQualified(new Path(dst_work, sCopy.getName()));
		//		try {
		//			Path dTmp = null == userUgi ? files.makeQualified(copy(sCopy, dst_work))
		//					: userUgi.doAs(new PrivilegedExceptionAction<Path>() {
		//						public Path run() throws Exception {
		//							return files.makeQualified(copy(sCopy, dst_work));
		//						};
		//					});
		//			Pattern pattern = null;
		//			String p = resource.getPattern();
		//			if (p != null) {
		//				pattern = Pattern.compile(p);
		//			}
		//			unpack(new File(dTmp.toUri()), new File(dFinal.toUri()), pattern);
		//			changePermissions(dFinal.getFileSystem(conf), dFinal);
		//			files.rename(dst_work, destDirPath, Rename.OVERWRITE);
		//		} catch (Exception e) {
		//			try {
		//				files.delete(destDirPath, true);
		//			} catch (IOException ignore) {
		//			}
		//			throw e;
		//		} finally {
		//			try {
		//				files.delete(dst_work, true);
		//			} catch (FileNotFoundException ignore) {
		//			}
		//			conf = null;
		//			resource = null;
		//		}
		return destDirPath;
	}

	//	/**
	//	 * Recursively change permissions of all files/dirs on path based 
	//	 * on resource visibility.
	//	 * Change to 755 or 700 for dirs, 555 or 500 for files.
	//	 * @param fs FileSystem
	//	 * @param path Path to modify perms for
	//	 * @throws IOException
	//	 * @throws InterruptedException 
	//	 */
	//	private void changePermissions(FileSystem fs, final Path path)
	//			throws IOException, InterruptedException {
	//		FileStatus fStatus = fs.getFileStatus(path);
	//		FsPermission perm = cachePerms;
	//		// set public perms as 755 or 555 based on dir or file
	//		if (resource.getVisibility() == LocalResourceVisibility.PUBLIC) {
	//			perm = fStatus.isDirectory() ? PUBLIC_DIR_PERMS : PUBLIC_FILE_PERMS;
	//		}
	//		// set private perms as 700 or 500
	//		else {
	//			// PRIVATE:
	//			// APPLICATION:
	//			perm = fStatus.isDirectory() ? PRIVATE_DIR_PERMS : PRIVATE_FILE_PERMS;
	//		}
	//		LOG.debug("Changing permissions for path " + path
	//				+ " to perm " + perm);
	//		final FsPermission fPerm = perm;
	//		if (null == userUgi) {
	//			files.setPermission(path, perm);
	//		}
	//		else {
	//			userUgi.doAs(new PrivilegedExceptionAction<Void>() {
	//				public Void run() throws Exception {
	//					files.setPermission(path, fPerm);
	//					return null;
	//				}
	//			});
	//		}
	//		if (fStatus.isDirectory()
	//				&& !fStatus.isSymlink()) {
	//			FileStatus[] statuses = fs.listStatus(path);
	//			for (FileStatus status : statuses) {
	//				changePermissions(fs, status.getPath());
	//			}
	//		}
	//	}

}
