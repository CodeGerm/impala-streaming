package org.cg.impala.streaming.compaction.operations;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.impala.streaming.ImpalaJDBCClient;
import org.cg.impala.streaming.compaction.CompactionContext;
import org.cg.impala.streaming.compaction.CompactionContext.States;
import org.cg.impala.streaming.compaction.Table;
import org.cg.impala.streaming.compaction.View;



public class InitState {

	private static final Log logger = LogFactory.getLog(InitState.class);
	
	public static CompactionContext init(ImpalaJDBCClient client, String tableName, String tableLocation, String managedLocation, String stateFilePath)
			throws SQLException, IOException {

		Table store_table1 = new Table(tableName, tableLocation);

		logger.info("creating store table 2");
		String storeTableName2 = tableName + "_store_2";
		Table store_table2 = new Table(storeTableName2, tableLocation);
		client.createStoringTable(storeTableName2, tableName, tableLocation);
		client.recoverPartition(storeTableName2);

		client.refresh(storeTableName2);
		client.invalidate(storeTableName2);

		logger.info("creating landing table 1");
		String landingTableName1 = tableName + "_landing_1";
		String landingTableDir1 = managedLocation + "/" + landingTableName1;
		Table landing_table1 = new Table(landingTableName1, landingTableDir1);
		client.createLandingTable(landingTableName1, tableName, landingTableDir1);

		logger.info("creating landing table 2");
		String landingTableName2 = tableName + "_landing_2";
		String landingTableDir2 = managedLocation + "/" + landingTableName2;
		Table landing_table2 = new Table(landingTableName2, landingTableDir2);
		client.createLandingTable(landingTableName2, tableName, landingTableDir2);

		Table landing_table = landing_table1;

		logger.info("creating view 1");
		List<Table> subTables1 = Arrays.asList(store_table1, landing_table1, landing_table2);
		List<String> subTablesName1 = Arrays.asList(store_table1.getName(), landing_table1.getName(),
				landing_table2.getName());
		String viewName1 = tableName + "_view_1";
		View view1 = new View(viewName1, null, subTables1);
		client.createView(viewName1, tableName, subTablesName1);
		client.refresh(viewName1);

		logger.info("creating view 2");
		List<Table> subTables2 = Arrays.asList(store_table2, landing_table2);
		List<String> subTablesName2 = Arrays.asList(store_table2.getName(), landing_table2.getName());
		String viewName2 = tableName + "_view_2";
		View view2 = new View(viewName2, null, subTables2);
		client.createView(viewName2, tableName, subTablesName2);
		client.refresh(viewName2);

		logger.info("creating exposed view");
		String viewName = tableName + "_view";
		List<View> subView = Arrays.asList(view1);
		List<String> subViewName = Arrays.asList(view1.getName());

		View view = new View(viewName, subView, null);

		client.createView(viewName, tableName, subViewName);
		client.refresh(viewName);

		States state = CompactionContext.States.StateI;
		CompactionContext context = new CompactionContext(view, view1, view2, landing_table, landing_table1,
				landing_table2, store_table1, store_table2, state, stateFilePath);
		
		logger.info("persist state to file");
		context.saveState();
		
		return context;

	}

}
