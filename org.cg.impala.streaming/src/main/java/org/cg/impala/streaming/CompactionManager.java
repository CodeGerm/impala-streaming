package org.cg.impala.streaming;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.impala.streaming.compaction.CompactionContext;
import org.cg.impala.streaming.compaction.operations.AddRecreatedLandingTableToView;
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

	private Gson gson;

	public CompactionManager(String config) throws IOException, ClassNotFoundException, SQLException {
		loadConfig();
		gson = new Gson();
		managedTables = new HashMap<String, CompactionContext>();
		loadContexts();
	}

	private void loadConfig() throws IOException, ClassNotFoundException, SQLException {
		Properties prop = new Properties();
		InputStream input = new FileInputStream("config.properties");
		prop.load(input);
		String connectionUrl = prop.getProperty("connectionUrl");
		String jdbcDriverName = prop.getProperty("jdbcDriverName");
		client = new ImpalaJDBCClient(connectionUrl, jdbcDriverName);
		stateFileLocation = prop.getProperty("stateFiles");

	}

	private String readFile(Path path) throws IOException {
		byte[] encoded = Files.readAllBytes(path);
		return new String(encoded);
	}

	private void loadContexts() {
		File stateFileDir = new File(stateFileLocation);

		// if the directory does not exist, create it
		if (!stateFileDir.exists()) {
			logger.info("creating directory: " + stateFileLocation);
			stateFileDir.mkdir();
		} else {
			if (!stateFileDir.isDirectory())
				throw new IllegalStateException("state file dir is not a directory");
			File[] listOfFiles = stateFileDir.listFiles();
			for (File file : listOfFiles) {
				if (file.isFile()) {
					String json;
					try {
						json = readFile(file.toPath());
						CompactionContext context = gson.fromJson(json, CompactionContext.class);
						managedTables.put(file.getName(), context);

					} catch (IOException e) {
						logger.error("can't load state context from file " + file.getName(), e);
					}
				}

			}

		}
	}

	public synchronized void next(CompactionContext context) throws SQLException, IOException {
		if (context.getState().equals(CompactionContext.States.StateI))
			SwitchLandingTable.run(context);
		if (context.getState().equals(CompactionContext.States.StateII))
			MoveDataFromLandingToPersist.run(client, context);
		if (context.getState().equals(CompactionContext.States.StateIII))
			SwitchViewToTempTable.run(client, context);
		if (context.getState().equals(CompactionContext.States.StateIV))
			RecreateOldLandingTable.run(client, context);
		if (context.getState().equals(CompactionContext.States.StateV))
			AddRecreatedLandingTableToView.run(client, context);
		if (context.getState().equals(CompactionContext.States.StateVI))
			SyncTwoPersistTable.run(client, context);
		if (context.getState().equals(CompactionContext.States.StateVII))
			RemoveLandingTableFromView.run(client, context);
	}

	public synchronized void compaction(String tableName) throws SQLException, IOException {
		if (managedTables == null)
			throw new IllegalStateException("manager not initialzed");
		CompactionContext context = managedTables.get(tableName);
		int stepNum = CompactionContext.States.values().length;
		for (int i = 0; i < stepNum; i++) {
			next(context);
			managedTables.put(tableName, context);
		}
	}
}
