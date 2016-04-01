package org.cg.impala.streaming.compaction.operations;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.impala.streaming.ImpalaJDBCClient;
import org.cg.impala.streaming.compaction.CompactionContext;


public class MoveDataFromLandingToPersist {
	private static final Log logger = LogFactory.getLog(MoveDataFromLandingToPersist.class);
	
	public static void run(ImpalaJDBCClient client,CompactionContext context) throws SQLException, IOException{
		//insert data from landing table 1 to store table 2
		client.compaction(context.getLandingTable1().getName(), context.getStore_table2().getName());
		//Update the row count stats
		client.updateStats(context.getStore_table2().getName());
		context.setState(CompactionContext.States.StateIII);
		context.saveState();
		logger.info("StateII to StateIII transition finished");
	}

}
