import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;

public class Main {
	public static void main(String[] args) {
		FileOutputStream outSTr = null;
		BufferedOutputStream Buff = null;

		int count = 1000;// д�ļ�����

		try {
			// �������ԣ�ufferedOutputStreamִ�к�ʱ:1,1��1 ����
			outSTr = new FileOutputStream(new File(
					"/Users/zhenglijiu/Desktop/test.txt"));
			Buff = new BufferedOutputStream(outSTr);
			for (int i = 0; i < count; i++) {
				Buff.write("����java �ļ�����\r\n".getBytes());
			}
			Buff.flush();
			Buff.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				Buff.close();
				outSTr.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
