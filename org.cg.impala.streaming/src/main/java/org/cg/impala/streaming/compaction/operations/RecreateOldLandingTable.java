package org.cg.impala.streaming.compaction.operations;


import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.impala.streaming.ImpalaJDBCClient;
import org.cg.impala.streaming.compaction.CompactionContext;


public class RecreateOldLandingTable {
	
	private static final Log logger = LogFactory.getLog(RecreateOldLandingTable.class);
	
	
	public static void run(ImpalaJDBCClient client,CompactionContext context) throws SQLException, IOException{
		//clean landing table 1, this has to be wait until all queries against view1 finished
		client.dropTable(context.getLandingTable1().getName());		
		client.createLandingTable(context.getLandingTable1().getName(), context.getLandingTable2().getName(), context.getLandingTable1().getDirectory());
		context.setState(CompactionContext.States.StateV);
		context.saveState();
		logger.info("StateIV to StateV transition finished");
	}

}
