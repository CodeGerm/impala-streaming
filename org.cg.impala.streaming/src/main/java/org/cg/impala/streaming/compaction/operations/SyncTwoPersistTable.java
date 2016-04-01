package org.cg.impala.streaming.compaction.operations;


import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.impala.streaming.ImpalaJDBCClient;
import org.cg.impala.streaming.compaction.CompactionContext;



public class SyncTwoPersistTable {
	private static final Log logger = LogFactory.getLog(SyncTwoPersistTable.class);

	public static void run(ImpalaJDBCClient client, CompactionContext context) throws SQLException, IOException{
		
		client.updateStats(context.getStore_table1().getName());
		context.setState(CompactionContext.States.StateVII);
		context.saveState();
		logger.info("StateVI to StateVII transition finished");
	}

}