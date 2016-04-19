package org.cg.impala.streaming.compaction.operations;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.impala.streaming.ImpalaJDBCClient;
import org.cg.impala.streaming.compaction.CompactionContext;
import org.cg.impala.streaming.compaction.View;



public class SwitchViewToTempTable {
	
	private static final Log logger = LogFactory.getLog(SwitchViewToTempTable.class);
	
	
	public static void run(ImpalaJDBCClient client,CompactionContext context) throws SQLException, IOException{
		//point view to view2 by altering the view in impala
		
		List<String>entities =new ArrayList<String>();
		entities.add(context.getView2().getName());
		client.alterView(context.getView().getName(), entities);
		
		//recreate view in context object
		List<View>subViews = new ArrayList<View>();
		subViews.add(context.getView2());
		View newView = new View(context.getView().getName(),subViews,null);
		context.setView(newView);

		context.setState(CompactionContext.States.StateIV);
		context.saveState();
		logger.info("StateIII to StateIV transition finished");
	}

}
