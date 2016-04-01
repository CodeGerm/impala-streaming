package org.cg.impala.streaming.compaction;

public class Table {

	private String name;

	private String directory;

	public Table(String name, String directory) {
		this.name = name;
		this.directory = directory;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDirectory() {
		return directory;
	}

	public void setDirectory(String directory) {
		this.directory = directory;
	}

}
