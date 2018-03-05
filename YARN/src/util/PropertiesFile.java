package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class PropertiesFile {
	private String filePath;
	
	public PropertiesFile(String filePath) {
		super();
		String temp = System.getProperty("user.dir").replace("\\", "/");
		
		String path = temp+"/src/";
		this.filePath = path+filePath;
		System.out.println(this.filePath);
	}

	

	/**     
	 * * д��properties��Ϣ     
	 * * @param filePath  ���·���������ļ���ͺ�׺��     
	 * * @param parameterName  ���     
	 * * @param parameterValue ֵ     
	 * */  
	public  void set(String parameterName,String parameterValue) {        
		Properties props = new Properties();       
		try {                             
			//����ļ������ڣ�����һ���µ�              
			File file=new File(filePath);                
			if(!file.exists()){              
				file.createNewFile();       
			}                    
			InputStream fis = new FileInputStream(filePath);        
			// ���������ж�ȡ�����б?���Ԫ�ضԣ�       
			props.load(fis);         
			fis.close();           
			OutputStream fos = new FileOutputStream(filePath);          
			props.setProperty(parameterName, parameterValue);         
			// ���ʺ�ʹ�� load �������ص� Properties ���еĸ�ʽ��          
			// ���� Properties ���е������б?���Ԫ�ضԣ�д�������       
			props.store(fos, parameterName);          
			fos.close(); // �ر���       
			} catch (IOException e) {        
				System.err.println("Visit "+filePath+" for updating "+parameterName+" value error");   
				}    
		}

	/**       
	 *  *        
	 *  * @Title: readValue         
	 *  * @Description: TODO  ͨ����·����ȡproperties�ļ����ԣ�  ���key��ȡvalue        
	 *  * @param filePath  properties�ļ����·���������ļ���ͺ�׺��        
	 *  * @param key   ����key        * @return String ����value        
	 *  */    
	public  String get(String key){         
		Properties props = new Properties();         
		InputStream in=null;      
		try{   
			//����ļ������ڣ�����һ���µ�              
			File file=new File(filePath);                
			if(!file.exists()){              
				file.createNewFile();       
			}                    
			in = new FileInputStream(filePath); 
			props.load(in);             
			String value = props.getProperty(key);           
			return value;                      
			}
		catch(Exception e){         
			e.printStackTrace();         
			return null;       
		    }
		finally{         
				try {          
					in.close();//-----------------------------------important        
					} 
				catch (IOException e) {             
						// TODO Auto-generated catch block            
						e.printStackTrace();           
					}   
				}   
		} 


}
