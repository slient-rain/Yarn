import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class Download extends HttpServlet {

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
//		String applicationId=request.getParameter("applicationId");
		String file=request.getParameter("file");
//		download(request,response,file,"e://");//+applicationId
		download(request,response,file,"e://");//+applicationId
		
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request,response);
	}

	public void download(HttpServletRequest request,
			HttpServletResponse response ,String downFilename,String dir){

		System.out.println("接收到下载请求");
		response.setContentType("application/octet-stream;charset=utf-8");
		response.setHeader("Location", downFilename);
		response.setHeader("Content-Disposition", "attachment; filename="
				+ downFilename);
		response.setCharacterEncoding("utf-8");

		ServletOutputStream out;
		try {
			out = response.getOutputStream();
			InputStream inStream = new FileInputStream(dir + downFilename);
			byte[] b = new byte[1024];
			int len;
			while ((len = inStream.read(b)) > 0)
				out.write(b, 0, len);
			response.setStatus(response.SC_OK);
			response.flushBuffer();
			out.close();
			inStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
