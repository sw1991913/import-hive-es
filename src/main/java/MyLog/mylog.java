package MyLog;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class mylog {
    public static String logbasedir;
    public static String Conf_Set_logpath = "probd.spark.log.path";
    public mylog() {
    }

    // 添加debug 的log日志
    public static void AddFixDebugLog(String str){
        FileWriter fw = null;

        try {
            File f = new File("/data/debug.log");
            fw = new FileWriter(f, true);
            PrintWriter pw = new PrintWriter(fw);
            pw.println(str);
            fw.flush();
            pw.close();
            fw.close();
        } catch (IOException var6) {
           var6.printStackTrace();
        }
    }
     //  获取 现在的时间
    public static String GetCurrTime() {
        String timestr = "";
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        timestr = sdf.format(date);
        return timestr;
    }
    // 从环境中获取 路径
    public static String GetPathFromEnv(String key) {
        String path = "";
        new HashMap();
        Map map = System.getenv();
        Iterator it = map.entrySet().iterator();

        while(it.hasNext()) {
            Entry entry = (Entry)it.next();
            if(key.equals(entry.getKey())) {
                path = (String)entry.getValue();
                break;
            }
        }

        return path;
    }
    // 是否能获取 log路径  LogBaseDir也就是说 Hadoop_home的绝对路径
    public static boolean GetLogBaseDir() {
        if(logbasedir == null) {
            String hadoopconf = GetPathFromEnv("HADOOP_HOME");
            if(hadoopconf == null || hadoopconf.isEmpty()) {
                logbasedir = "";
                System.out.println("hadoopconf: error");
                return false;
            }
              //这里是 根据 找到的xml文件 找到 log根目录
            hadoopconf = hadoopconf + "/etc/hadoop/pro_loginfo.xml";
            logbasedir = GetConfMark(hadoopconf, Conf_Set_logpath);
            System.out.println("GetLogBaseDir:" + logbasedir + " hadoopconf: " + hadoopconf);
        }

        if (logbasedir.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }
    //添加  日志
    public static void AddLog(String filename, String buffer) {
            String pathname = filename;
        if(GetLogBaseDir()) {
            if(!filename.startsWith(logbasedir)) {
                pathname = logbasedir + "/" + filename;
            }

            FileWriter fw = null;

            try {
                File f = new File(pathname);
                fw = new FileWriter(f, true);
                PrintWriter pw = new PrintWriter(fw);
                pw.println(buffer);
                fw.flush();
                pw.close();
                fw.close();
            } catch (IOException var6) {
                var6.printStackTrace();
            }

        }
    }
    //添加 导入日志
    public static void AddImportLog(String path, String type, long filelen, long timelen) {
        String pathname = path;
        if(GetLogBaseDir()) {
            if(!path.startsWith(logbasedir)) {
                pathname = logbasedir + "/" + path;
            }

            String timestr = GetCurrTime();
            timestr = timestr + "\t";
            timestr = timestr + type;
            timestr = timestr + "\t";
            timestr = timestr + path;
            timestr = timestr + "\t";
            timestr = timestr + Long.toString(filelen);
            timestr = timestr + "\t";
            timestr = timestr + Long.toString(timelen);
            System.out.println("AddImportLog:" + timestr);
            AddLog(pathname, timestr);
        }
    }
    //添加 正常日志
    public static void AddNormalLog(String path, String title, String content) {
        String pathname = path;
        if(GetLogBaseDir()) {
            if(!path.startsWith(logbasedir)) {
                pathname = logbasedir + "/" + path;
            }

            String timestr = GetCurrTime();
            timestr = timestr + "\t";
            timestr = timestr + title;
            timestr = timestr + "\t";
            timestr = timestr + content;
            System.out.println("AddNormalLog:" + timestr);
            AddLog(pathname, timestr);
        }
    }
    //  设置xml参数
    public static void setConfMark(String confFile, String name, String value) {
        System.out.println("setConfMark  confFile " + confFile + " name:" + name + "  value " + value);
        if(confFile != null) {
            try {
                SAXReader saxReader = new SAXReader();
                File fileConf = new File(confFile);
                Document docConf = saxReader.read(fileConf);
                Element xmlRoot = docConf.getRootElement();
                Element xmlName = null;
                Element xmlVal = null;
                List xmlPropertyList = xmlRoot.elements("property");
                Iterator var11 = xmlPropertyList.iterator();

                Element xmlWriter;
                while(var11.hasNext()) {
                    xmlWriter = (Element)var11.next();
                    xmlName = xmlWriter.element("name");
                    if(xmlName != null) {
                        if(xmlName.getText().equalsIgnoreCase(name)) {
                            xmlVal = xmlWriter.element("value");
                            if(xmlVal == null) {
                                xmlVal = xmlWriter.addElement("value");
                            }
                            xmlVal.setText(value);
                            break;
                        }
                        xmlName = null;
                    }
                }

                if(xmlName == null) {
                    xmlWriter = xmlRoot.addElement("property");
                    xmlName = xmlWriter.addElement("name");
                    xmlName.setText(name);
                    xmlVal = xmlWriter.addElement("value");
                    xmlVal.setText(value);
                    System.out.println("run.xmlProperty=" + xmlWriter);
                }

                System.out.println("run.docConf=" + docConf);
                XMLWriter xmlWriter1 = new XMLWriter(new FileWriter(confFile));
                xmlWriter1.write(docConf);
                xmlWriter1.close();
            } catch (Exception var12) {
                var12.printStackTrace();
            }

        }
    }

    //  获取xml参数
    public static String GetConfMark(String confFile, String name) {
        String result="";
        if (confFile == null) return result;
        try {
            SAXReader saxReader = new SAXReader();
            File fileConf = new File(confFile);
            Document docConf = saxReader.read(fileConf);
            Element xmlRoot = docConf.getRootElement();

            Element xmlName = null, xmlVal = null;
            List<Element> xmlPropertyList = xmlRoot.elements("property");
            for (Element xmlProperty : xmlPropertyList) {
                xmlName = xmlProperty.element("name");
                if (xmlName == null) continue;
                if (xmlName.getText().equalsIgnoreCase(name)) {
                    xmlVal=xmlProperty.element("value");
                    result=xmlVal.getText();
                    break;
                }
                xmlName = null;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }



}
