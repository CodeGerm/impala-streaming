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



public class AddRecreatedLandingTableToView {
	
	private static final Log logger = LogFactory.getLog(AddRecreatedLandingTableToView.class);
	
	
	
	public static void run(ImpalaJDBCClient client, CompactionContext context) throws SQLException, IOException{
		
		//include landing table 1 in view 2 by recreating the view 2 in impala
		client.dropView(context.getView2().getName());
		List<String>entities =new ArrayList<String>();
		entities.add(context.getStore_table2().getName());
		entities.add(context.getLandingTable1().getName());
		entities.add(context.getLandingTable2().getName());
		client.createView(context.getView2().getName(),context.getView1().getName(),entities);
		
		//recreate view 2 in context object
		List<Table>subTables = new ArrayList<Table>();
		subTables.add(context.getStore_table2());
		subTables.add(context.getLandingTable1());
		subTables.add(context.getLandingTable2());
		View newView2 = new View(context.getView2().getName(),null,subTables);
		context.setView2(newView2);
		
		//view2 changed, so view in context should update too
		List<View>subViews = new ArrayList<View>();
		subViews.add(newView2);
		View newView = new View(context.getView().getName(),subViews,null);
		context.setView(newView);
		logger.info("StateV to StateVI transition finished");
		
		
		context.setState(CompactionContext.States.StateVI);
		context.saveState();
	}

}
