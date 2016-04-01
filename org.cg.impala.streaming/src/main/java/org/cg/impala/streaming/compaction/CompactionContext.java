package org.cg.impala.streaming.compaction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.google.gson.Gson;

public class CompactionContext {

	private View view;

	private View view1;

	private View view2;

	private Table exposed_landing_table;

	private Table landing_table1;

	private Table landing_table2;

	private Table store_table1;

	private Table store_table2;

	private States state;
	
	private String stateFilePath;

	public static enum States {
		StateI, StateII, StateIII, StateIV, StateV, StateVI, StateVII
	};

	public void swapEntity() {
		View tmpView1 = this.view1;
		this.view1 = this.view2;
		this.view2 = tmpView1;
		Table tmpLandingTable = this.landing_table1;
		this.landing_table1 = this.landing_table2;
		this.landing_table2 = tmpLandingTable;
		Table tmpStoreTable = this.store_table1;
		this.store_table1 = this.store_table2;
		this.store_table2 = tmpStoreTable;
		
		
	}

	public View getView() {
		return view;
	}

	public void setView(View view) {
		this.view = view;
	}

	public View getView1() {
		return view1;
	}

	public void setView1(View view1) {
		this.view1 = view1;
	}

	public View getView2() {
		return view2;
	}

	public void setView2(View view2) {
		this.view2 = view2;
	}

	public Table getLandingTable() {
		return exposed_landing_table;
	}

	public void setLandingTable(Table landing_table) {
		this.exposed_landing_table = landing_table;
	}

	public Table getLandingTable1() {
		return landing_table1;
	}

	public void setLandingTable1(Table landing_table1) {
		this.landing_table1 = landing_table1;
	}

	public Table getLandingTable2() {
		return landing_table2;
	}

	public void setLandingTable2(Table landing_table2) {
		this.landing_table2 = landing_table2;
	}

	public String getStateFilePath() {
		return stateFilePath;
	}

	public void setStateFilePath(String stateFilePath) {
		this.stateFilePath = stateFilePath;
	}

	public States getState() {
		return state;
	}

	public void setState(States state) {
		this.state = state;
	}
	
	
	public void saveState() throws IOException {
		
		Gson gson =new Gson(); 
		String objectString = gson.toJson(this);
		File stateFile = new File(stateFilePath);
		FileWriter fw =new FileWriter(stateFile, false);
		fw.write(objectString);
		fw.close();

	}

	public Table getStore_table1() {
		return store_table1;
	}

	public void setStore_table1(Table store_table1) {
		this.store_table1 = store_table1;
	}

	public Table getStore_table2() {
		return store_table2;
	}

	public void setStore_table2(Table store_table2) {
		this.store_table2 = store_table2;
	}
	
	public CompactionContext(){
		
	}

	public CompactionContext(View view, View view1, View view2, Table exposed_landing_table, Table landing_table1,
			Table landing_table2, Table store_table1, Table store_table2, States state, String stateFilePath) {
		this.view = view;
		this.view1 = view1;
		this.view2 = view2;
		this.exposed_landing_table = exposed_landing_table;
		this.landing_table1 = landing_table1;
		this.landing_table2 = landing_table2;
		this.store_table1 = store_table1;
		this.store_table2 = store_table2;
		this.state = state;
		this.stateFilePath = stateFilePath;
	}

}
