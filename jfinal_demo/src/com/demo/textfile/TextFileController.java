package com.demo.textfile;

import com.jfinal.core.Controller;

public class TextFileController extends Controller{
 public void index(){
	 render("textfile.html");
 }
 public void run(){
	 
	 String dataType = getPara("dataType");
	 String databaseAddress = getPara("databaseAddress");
	 String userName = getPara("userName");
	 String databaseName = getPara("databaseName");
	 String passWord = getPara("passWord");
	 String fileChaset = getPara("fileChaset");
	 String hdfsDir = getPara("hdfsDir");
	 String strIskeepDirectoryStruct = getPara("strIskeepDirectoryStruct");
	 String strIsOverWrite = getPara("strIsOverWrite");
	 
	 setAttr("dataType", dataType);
	 setAttr("databaseAddress", databaseAddress);
	 setAttr("userName", userName);
	 setAttr("databaseName", databaseName);
	 setAttr("passWord", passWord);
	 setAttr("fileChaset", fileChaset);
	 setAttr("hdfsDir", hdfsDir);
	 setAttr("strIskeepDirectoryStruct", strIskeepDirectoryStruct);
	 setAttr("strIsOverWrite", strIsOverWrite);
	 
	 System.out.println(dataType);
	 System.out.println(databaseAddress);
	 System.out.println(userName);
	 System.out.println(databaseName);
	 System.out.println(passWord);
	 System.out.println(fileChaset);
	 System.out.println(hdfsDir);
	 System.out.println(strIskeepDirectoryStruct);
	 System.out.println(strIsOverWrite);
	 
	 render("textfile.html");
 }
}
