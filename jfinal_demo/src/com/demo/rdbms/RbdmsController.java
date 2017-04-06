package com.demo.rdbms;

import com.demo.common.model.Blog;
import com.jfinal.core.Controller;

public class RbdmsController extends Controller{

	public void index() {
//		setAttr("blogPage", Blog.me.paginate(getParaToInt(0, 1), 10));
		render("rdbms.html");
	}
	public void run(){
		String dataType = getPara("dataType");
		String databaseAddress = getPara("databaseAddress");
		String userName = getPara("userName");
		String databaseName = getPara("databaseName");
		String passWord = getPara("passWord");
		String lineSeparator = getPara("lineSeparator");
		String fieldSeparator = getPara("fieldSeparator");
		String loadType = getPara("loadType");
		String excludeTables = getPara("excludeTables");
		String querySql = getPara("querySql");
		String tableNames = getPara("tableNames");
		String tableColumns = getPara("tableColumns");
		String splitField = getPara("splitField");
		String primaryKey = getPara("primaryKey");
		String mapTasks = getPara("mapTasks");
		String hdfsDir = getPara("hdfsDir");
		
		setAttr("dataType", dataType);
		setAttr("databaseAddress", databaseAddress);
		setAttr("userName", userName);
		setAttr("databaseName", databaseName);
		setAttr("passWord", passWord);
		setAttr("lineSeparator", lineSeparator);
		setAttr("fieldSeparator", fieldSeparator);
		setAttr("loadType", loadType);
		setAttr("excludeTables", excludeTables);
		setAttr("querySql", querySql);
		setAttr("tableNames", tableNames);
		setAttr("tableColumns", tableColumns);
		setAttr("splitField", splitField);
		setAttr("primaryKey", primaryKey);
		setAttr("mapTasks", mapTasks);
		setAttr("hdfsDir", hdfsDir);
		
		System.out.println(dataType);
		System.out.println(databaseAddress);
		System.out.println(userName);
		System.out.println(databaseName);
		System.out.println(passWord);
		System.out.println(lineSeparator);
		System.out.println(fieldSeparator);
		System.out.println(loadType);
		System.out.println(excludeTables);
		System.out.println(querySql);
		System.out.println(tableNames);
		System.out.println(splitField);
		System.out.println(primaryKey);
		System.out.println(mapTasks);
		System.out.println(hdfsDir);
		render("rdbms.html");
	}
	public void add() {
	}
	
//	@Before(BlogValidator.class)
	public void save() {
//		getModel(Blog.class).save();
//		redirect("/Rdbms");
	}
	
	public void edit() {
//		setAttr("Rdbms", Blog.me.findById(getParaToInt()));
	}
	
//	@Before(BlogValidator.class)
	public void update() {
//		getModel(Blog.class).update();
//		redirect("/Rdbms");
	}
	
	

}
