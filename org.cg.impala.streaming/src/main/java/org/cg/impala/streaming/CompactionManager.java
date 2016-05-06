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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.impala.streaming.compaction.CompactionContext;
import org.cg.impala.streaming.compaction.CompactionStatus;
import org.cg.impala.streaming.compaction.View;
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

	private Map<String, Boolean> loadingStatus;

	private Map<String, CompactionStatus> compactionStatus;

	private ImpalaJDBCClient client;

	private String stateFileLocation;

	private String tmpTableLocation;

	private String hdfsConnection;
	
	private String connectionUrl;
	
	private String jdbcDriverName;

	private Gson gson;

	private Integer defaultCompactionTaskNumLimit=1000;

	private static String IMPALA_USER_DIRECTORY = "/user/impala";

	private static String HADOOP_USER_NAME = "HADOOP_USER_NAME";

	private static String IMPAlA = "impala";

	private Boolean hdfs_checked = false;


	public CompactionManager(String config) throws IOException, ClassNotFoundException, SQLException {
		loadConfig(config);
		gson = new Gson();
		managedTables = new HashMap<String, CompactionContext>();
		loadingStatus = new HashMap<String, Boolean>();
		compactionStatus = new LinkedHashMap<String, CompactionStatus>(defaultCompactionTaskNumLimit) {
			private static final long serialVersionUID = 1L;
			@Override protected boolean removeEldestEntry(Map.Entry<String, CompactionStatus> entry) {
				return size() > defaultCompactionTaskNumLimit;
			}
		}; 
		loadContexts();
		logger.info("Compaction manager initialized!");
	}

	private void loadConfig(String config) throws IOException, ClassNotFoundException, SQLException {
		Properties prop = new Properties();
		InputStream input = new FileInputStream(config);
		prop.load(input);
		loadConfig(prop);

	}

	private void tableExistCheck(String tableName) {
		if(!managedTables.containsKey(tableName)){
			String message =tableName + " table not managed by compaction manger yet!"; 
			logger.error(message);
			throw new IllegalArgumentException(message);
		}	
	}

	private void checkHdfs() throws IOException{
		if(!hdfs_checked){
			System.setProperty(HADOOP_USER_NAME, IMPAlA);
			HdfsClient dfs = new HdfsClient(hdfsConnection);
			if(!dfs.checkDir(IMPALA_USER_DIRECTORY)){
				logger.warn("impala home directory not exist, creating...");
				dfs.mkDir(IMPALA_USER_DIRECTORY);
				logger.info("impala home directory created");
			}

			String tmpTableLocationPath = tmpTableLocation.replace(hdfsConnection, "");
			if(!dfs.checkDir(tmpTableLocationPath)){
				logger.warn("temperory table directory not exist, creating...");
				dfs.mkDir(tmpTableLocationPath);
				logger.info("temperory table directory: "+tmpTableLocationPath+" created ");
			}

			if(!dfs.checkDir(IMPALA_USER_DIRECTORY)){
				logger.warn("impala home directory not exist, creating...");
				dfs.mkDir(IMPALA_USER_DIRECTORY);
			}
			dfs.close();
		}

		hdfs_checked = true;

	}

	private void loadConfig(Properties prop) throws IOException, ClassNotFoundException, SQLException {

		hdfsConnection = prop.getProperty(HdfsClient.HDFS_CONNECTION_NAME);

		tmpTableLocation = prop.getProperty("tmpTableLocation");


		connectionUrl = prop.getProperty("connectionUrl");
		jdbcDriverName = prop.getProperty("jdbcDriverName");
		//client = new ImpalaJDBCClient(connectionUrl, jdbcDriverName);
		stateFileLocation = prop.getProperty("stateFiles");

		logger.info("state file location: "+stateFileLocation);
		logger.info("connection Url: "+connectionUrl);


	}
	
	private void initJdbcIfNotExist() throws IOException, SQLException{
		if(client == null)
			try {
				client = new ImpalaJDBCClient(connectionUrl, jdbcDriverName);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
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
		checkHdfs();
		initJdbcIfNotExist();
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


	public  List<String> listTables() throws SQLException, IOException{
		initJdbcIfNotExist();
		List<String> tables = new ArrayList<String>();
		tables.addAll(managedTables.keySet());
		return tables;
	}


	public  CompactionContext getTableContext(String tableName) throws SQLException{
		
		tableExistCheck(tableName);
		return managedTables.get(tableName);
	}

	public  String getTableState(String tableName) throws SQLException{
		tableExistCheck(tableName);
		return managedTables.get(tableName).getState().toString();
	}

	public CompactionStatus getCompactionStatus(String id){
		CompactionStatus cs =  compactionStatus.get(id);
		if(cs == null){
			String message ="compaction: "+id + " does not exist"; 
			logger.error(message);
			throw new IllegalArgumentException(message);
		}
		return compactionStatus.get(id);
	}

	public synchronized void runNext(String tableName) throws SQLException, IOException, InterruptedException {
		
		tableExistCheck(tableName);
		initJdbcIfNotExist();
		CompactionContext context = managedTables.get(tableName);
		if (context.getState().equals(CompactionContext.States.StateI))
			SwitchLandingTable.run(context);
		else if (context.getState().equals(CompactionContext.States.StateII)){
			MoveDataFromLandingToPersist.run(client, context);
		}
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

	public synchronized CompactionStatus compaction(String tableName) throws SQLException, IOException, InterruptedException {
		checkHdfs();
		tableExistCheck(tableName);
		initJdbcIfNotExist();
		if(!getTableState(tableName).equals(CompactionContext.States.StateI.toString())){
			String message =tableName + " is not in normal state, please wait or recover"; 
			logger.error(message);
			throw new IllegalStateException(message);
		}
		int stepNum = CompactionContext.States.values().length;
		runNext(tableName);
		String newLandingTable = getLandingTable(tableName);
		String id = UUID.randomUUID().toString();
		CompactionStatus cs = new CompactionStatus(id, newLandingTable, getTableState(tableName), tableName, CompactionStatus.Status.running);
		compactionStatus.put(id, cs);
		Thread thread = new Thread("compactionThread"){
			public void run(){
				for(int i=0;i<stepNum-1;i++){
					try {
						runNext(tableName);
						cs.setTableState(getTableState(tableName));
						compactionStatus.put(id, cs);
					} catch (SQLException | IOException | InterruptedException e) {
						cs.setFailedReason(e.getMessage());
						cs.setStatus(CompactionStatus.Status.failed);
						compactionStatus.put(id, cs);
						logger.error(e);
					}	
				}
				cs.setStatus(CompactionStatus.Status.finished);
				compactionStatus.put(id, cs);
				logger.info("Compaction finished");
			}
		};
		thread.start();

		//return newLandingTable;
		return cs;
	}



	public void recover(String tableName) throws SQLException, IOException, InterruptedException {

		tableExistCheck(tableName);
		initJdbcIfNotExist();
		int stepNum = CompactionContext.States.values().length;
		for (int i = 0; i < stepNum; i++) {
			runNext(tableName);
			if(managedTables.get(tableName).getState().equals(CompactionContext.States.StateI))
				break;
		}
	}

	public synchronized void close() throws SQLException{
		if(client!=null)
			client.close();
	}

	public String getLandingTable(String tableName) throws SQLException{
		tableExistCheck(tableName);
		return managedTables.get(tableName).getLandingTable().getDirectory();
	}

	public View getView(String tableName) throws SQLException{
		tableExistCheck(tableName);
		return managedTables.get(tableName).getView();
	}

	public  boolean getLoadingState(String tableName) throws SQLException{
		if(!loadingStatus.containsKey(tableName)) 
			return false;
		return loadingStatus.get(tableName);
	}

	public  void load(String tableName) throws SQLException, IOException{
		tableExistCheck(tableName);
		checkHdfs();
		loadingStatus.put(tableName, true);
		String landingTable = managedTables.get(tableName).getLandingTable().getName();
		client.recoverPartition(landingTable);
		client.refresh(landingTable);
		loadingStatus.put(tableName, false);
	}

	public synchronized void dropTable(String tableName) throws SQLException{
		String view1 = tableName+"_view_1";
		String view2 = tableName+"_view_2";
		String landing1 = tableName+"_landing_1";
		String landing2 = tableName+"_landing_2";
		String view = tableName+"_view";
		client.dropTable(landing2);
		client.dropTable(landing1);
		client.dropView(view);
		client.dropView(view1);
		client.dropView(view2);

	}

	public static void main(String args[]){

	}


}
