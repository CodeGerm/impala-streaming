package org.cg.impala.streaming.compaction;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;


public class CompactionContextTest {
	
	private CompactionContext context;


	@Before
	public void setUp() {
		
		Table store_table1 = new Table("store_table1", "hdfs:///user/hive/warehouse/compactionTest_store_table");
		Table store_table2 = new Table("store_table2", "hdfs:///user/hive/warehouse/compactionTest_store_table");
		
		Table landing_table1 = new Table("landing_table1", "hdfs:///user/hive/warehouse/compactionTest_landing_table1");
		Table landing_table2 = new Table("landing_table2", "hdfs:///user/hive/warehouse/compactionTest_landing_table2");
		
		Table landing_table = landing_table1;
		
		List<Table> subTables1=new ArrayList<Table>();
		subTables1.add(store_table1);
		subTables1.add(landing_table1);
		subTables1.add(landing_table2);
		View view1 = new View("view1", null, subTables1);
		
		List<Table> subTables2=new ArrayList<Table>();
		subTables2.add(store_table2);
		subTables2.add(landing_table2);
		View view2 = new View("view2", null, subTables1);
		
		View view = view1;
		
		context= new CompactionContext();
		context.setLandingTable(landing_table);
		context.setLandingTable1(landing_table1);
		context.setLandingTable2(landing_table2);
		
		context.setStore_table1(store_table1);
		context.setStore_table2(store_table2);
		context.setView(view);
		context.setView1(view1);
		context.setView2(view2);
		
		
		context.setState(CompactionContext.States.StateI);
		context.setStateFilePath("CompactionContextTest.state");
		
		
	}
	
	
	@Test
	public void save() throws IOException {
		Gson gson = new Gson();
		context.saveState();
		
		File file = new File(context.getStateFilePath());
		byte[] encoded = Files.readAllBytes(file.toPath());
		String json = new String(encoded);
		CompactionContext newContext = gson.fromJson(json, CompactionContext.class);
		
		assertEquals(context.getState(), newContext.getState());
		
	}
		


}
