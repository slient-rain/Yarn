package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import client.YarnClientImpl;

public class PropertiesFile {

	private static final Logger LOG = LoggerFactory
			.getLogger(YarnClientImpl.class);
	private Properties pf = new Properties();

	public PropertiesFile(String filePath) {
		InputStream isInputStream = YarnClientImpl.class.getClassLoader()
				.getResourceAsStream(filePath);
		try {
			pf.load(isInputStream);
		} catch (IOException e) {
			LOG.error(e.toString());
		}
	}

	/**
	 * * д��properties��Ϣ * @param filePath ���·���������ļ���ͺ�׺�� * @param
	 * parameterName ��� * @param parameterValue ֵ
	 * */
	public void set(String parameterName, String parameterValue) {
		pf.setProperty(parameterName, parameterValue);
	}

	/**
	 * * * @Title: readValue * @Description: TODO ͨ����·����ȡproperties�ļ����ԣ�
	 * ���key��ȡvalue * @param filePath properties�ļ����·���������ļ���ͺ�׺�� * @param
	 * key ����key * @return String ����value
	 * */
	public String get(String key) {
		return pf.get(key).toString();
	}

}
