package org.cg.impala.streaming.compaction.operations;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.impala.streaming.ImpalaJDBCClient;
import org.cg.impala.streaming.compaction.CompactionContext;
import org.cg.impala.streaming.compaction.Table;
import org.cg.impala.streaming.compaction.View;



public class RemoveLandingTableFromView {
	private static final Log logger = LogFactory.getLog(RemoveLandingTableFromView.class);

	public static void run(ImpalaJDBCClient client, CompactionContext context) throws SQLException, IOException{
		//exclude landing table 2 in view 1 by altering the view 1 in impala
		
		List<String>entities =new ArrayList<String>();
		entities.add(context.getStore_table1().getName());
		entities.add(context.getLandingTable1().getName());
		client.alterView(context.getView1().getName(), entities);

		//recreate view 1 in context object
		List<Table>subTables = new ArrayList<Table>();
		subTables.add(context.getStore_table1());
		subTables.add(context.getLandingTable1());
		View newView = new View(context.getView1().getName(),null,subTables);
		context.setView1(newView);

		//for next compaction iteration
		context.swapEntity();
		
		context.setState(CompactionContext.States.StateI);
		context.saveState(); 
		logger.info("StateVII to StateI transition finished");
	}

}