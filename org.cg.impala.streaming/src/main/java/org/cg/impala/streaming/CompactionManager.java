package org.cg.impala.streaming;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;
import org.cg.impala.streaming.compaction.CompactionContext;
import org.cg.impala.streaming.compaction.operations.AddRecreatedLandingTableToView;
import org.cg.impala.streaming.compaction.operations.InitState;
import org.cg.impala.streaming.compaction.operations.MoveDataFromLandingToPersist;
import org.cg.impala.streaming.compaction.operations.RecreateOldLandingTable;
import org.cg.impala.streaming.compaction.operations.RemoveLandingTableFromView;
import org.cg.impala.streaming.compaction.operations.SwitchLandingTable;
import org.cg.impala.streaming.compaction.operations.SwitchViewToTempTable;
import org.cg.impala.streaming.compaction.operations.SyncTwoPersistTable;

import com.google.gson.Gson;

public class CompactionManager {

	private static final Log logger = LogFactory.getLog(CompactionManager.class);

	private Map<String, CompactionContext> managedTables;

	private ImpalaJDBCClient client;

	private String stateFileLocation;
	
	private String tmpTableLocation;

	private Gson gson;

	public CompactionManager(String config) throws IOException, ClassNotFoundException, SQLException {
		loadConfig(config);
		gson = new Gson();
		managedTables = new HashMap<String, CompactionContext>();
		loadContexts();
		logger.info("Compaction manager initialized!");
	}

	private void loadConfig(String config) throws IOException, ClassNotFoundException, SQLException {
		Properties prop = new Properties();
		InputStream input = new FileInputStream(config);
		prop.load(input);
		loadConfig(prop);

	}
	
	private void loadConfig(Properties prop) throws IOException, ClassNotFoundException, SQLException {
		
		String connectionUrl = prop.getProperty("connectionUrl");
		String jdbcDriverName = prop.getProperty("jdbcDriverName");
		client = new ImpalaJDBCClient(connectionUrl, jdbcDriverName);
		stateFileLocation = prop.getProperty("stateFiles");
		tmpTableLocation = prop.getProperty("tmpTableLocation");
		logger.info("state file location: "+stateFileLocation);
		logger.info("connection Url: "+connectionUrl);
		

	}

	private String readFile(Path path) throws IOException {
		byte[] encoded = Files.readAllBytes(path);
		return new String(encoded);
	}

	private void loadContexts() {
		File stateFileDir = new File(stateFileLocation);

		// if the directory does not exist, create it
		if (!stateFileDir.exists()) {
			logger.info("Creating state files' directory: " + stateFileLocation);
			stateFileDir.mkdir();
		} else {
			if (!stateFileDir.isDirectory())
				throw new IllegalStateException("State file dir is not a directory");
			File[] listOfFiles = stateFileDir.listFiles();
			for (File file : listOfFiles) {
				if (file.isFile()) {
					String json;
					try {
						json = readFile(file.toPath());
						CompactionContext context = gson.fromJson(json, CompactionContext.class);
						String tableName = null;
						if(file.getName().contains(".")){
							String [] content = file.getName().split("\\.");
							tableName = content[0];
						} else {
							tableName = file.getName();
						}
						managedTables.put(tableName, context);

					} catch (IOException e) {
						logger.error("can't load state context from file " + file.getName(), e);
					}
				}

			}

		}
	}
	
	public synchronized void addTable(String tableName) throws SQLException, IOException{
		if(managedTables.containsKey(tableName)){
			String message =tableName + " table already managed by compaction manger!"; 
			logger.error(message);
			throw new IllegalArgumentException(message);
		}	
		logger.info("Adding table " + tableName + " to compaction manager");
		String tableLocation = client.getTableLocation(tableName);
		
		if(tableLocation == null){
			String message = tableName + " is not a table or does not exist ";
			logger.error(message);
			throw new IllegalArgumentException(message);
		}
			
		CompactionContext context = InitState.init(client, tableName, tableLocation, tmpTableLocation, stateFileLocation+"/"+tableName+".state");
		
		managedTables.put(tableName, context);
	}
	
	
	public synchronized List<String> listTables() throws SQLException, IOException{
		List<String> tables = new ArrayList<String>();
		tables.addAll(managedTables.keySet());
		return tables;
		
	}

	public synchronized void runNext(String tableName) throws SQLException, IOException {
		CompactionContext context = managedTables.get(tableName);

		if (context.getState().equals(CompactionContext.States.StateI))
			SwitchLandingTable.run(context);
		else if (context.getState().equals(CompactionContext.States.StateII))
			MoveDataFromLandingToPersist.run(client, context);
		else if (context.getState().equals(CompactionContext.States.StateIII))
			SwitchViewToTempTable.run(client, context);
		else if (context.getState().equals(CompactionContext.States.StateIV))
			RecreateOldLandingTable.run(client, context);
		else if (context.getState().equals(CompactionContext.States.StateV))
			AddRecreatedLandingTableToView.run(client, context);
		else if (context.getState().equals(CompactionContext.States.StateVI))
			SyncTwoPersistTable.run(client, context);
		else if (context.getState().equals(CompactionContext.States.StateVII))
			RemoveLandingTableFromView.run(client, context);
		managedTables.put(tableName, context);
	}

	public synchronized void compaction(String tableName) throws SQLException, IOException {
		if (managedTables == null)
			throw new IllegalStateException("manager not initialzed");
		int stepNum = CompactionContext.States.values().length;
		for (int i = 0; i < stepNum; i++) {
			runNext(tableName);
			
		}
	}
	
	public synchronized void close() throws SQLException{
		client.close();
	}
	
	

}
