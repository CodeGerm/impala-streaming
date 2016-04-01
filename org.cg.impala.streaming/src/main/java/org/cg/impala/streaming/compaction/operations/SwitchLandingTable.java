package org.cg.impala.streaming.compaction.operations;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.impala.streaming.compaction.CompactionContext;
	

public class SwitchLandingTable {

	private static final Log logger = LogFactory.getLog(SwitchLandingTable.class);
	
	public static void run(CompactionContext context) throws IOException{
		//pointing ingestion to landing table 2
		context.setLandingTable(context.getLandingTable2());
		context.setState(CompactionContext.States.StateII);
		context.saveState();
		logger.info("StateI to StateII transition finished");
	}

}
