package org.cg.impala.streaming.compaction;

import java.util.List;

public class View {
	
	
	private String name;
	private List<View> subViews;
	private List<Table> subTables;
	
	public View(String name, List<View> subViews, List<Table> subTables) {
		this.name = name;
		this.subViews = subViews;
		this.subTables = subTables;
	}
	
	public List<View> getSubViews() {
		return subViews;
	}
	public void setSubViews(List<View> subViews) {
		this.subViews = subViews;
	}
	public List<Table> getSubTables() {
		return subTables;
	}
	public void setSubTables(List<Table> subTables) {
		this.subTables = subTables;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "View [name=" + name + ", subViews=" + subViews + ", subTables=" + subTables + "]";
	}
	
	

}
