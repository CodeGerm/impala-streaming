package org.cg.impala.streaming.compaction.operations;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.cg.impala.streaming.ImpalaJDBCClient;
import org.cg.impala.streaming.compaction.CompactionContext;
import org.cg.impala.streaming.compaction.CompactionContext.States;
import org.cg.impala.streaming.compaction.Table;
import org.cg.impala.streaming.compaction.View;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;

public class OperationTest {

	private static String TEST_TABLE_NAME = "impala_compaction_unit_test";
	private static String TEST_TABLE_LOCATION = "/user/hive/warehouse"+TEST_TABLE_NAME;
	private static String MANAGED_LOCATION = "/user/hive/warehouse";
	private static String STATE_FILE = TEST_TABLE_NAME+".state";
	private static String impalaHost = "localhost";
	private static int impalaPort = 21050;

	private ImpalaJDBCClient client;
	private CompactionContext context;

	public static boolean isPortOpen (String host,int port) {
		Socket s = null;
		try
		{
			s = new Socket(host, port);
			return true;
		}
		catch (Exception e)
		{
			return false;
		}
		finally
		{
			if(s != null)
				try {s.close();}
			catch(Exception e){}
		}
	}
	
	public static boolean isHostReachable (String host) {
		InetAddress address;
		try {
			address = InetAddress.getByName(host);
			return address.isReachable(1000);
		} catch (Exception e) {
			return false;
		}
	}

	private static void createTestTable(ImpalaJDBCClient client) throws SQLException{
		String sqlStatement = "create table if not exists "+TEST_TABLE_NAME+" (uid int, name string)  stored as parquetfile ";
		client.runUpdateStatement(sqlStatement);
	}

	private static void validateStateFile(CompactionContext context, States state) throws IOException{
		Gson gson =new Gson();
		File file = new File(context.getStateFilePath());
		byte[] encoded = Files.readAllBytes(file.toPath());
		String json = new String(encoded);
		CompactionContext newContext = gson.fromJson(json, CompactionContext.class);
		Assert.assertEquals(newContext.getState(), state);
	}

	public static List<String> getViewSubEntity(ImpalaJDBCClient client, String viewName) throws SQLException{
		try{
			String sqlcommand ="describe formatted "+viewName;
			List<String> subStructures = new ArrayList<String>();
			ResultSet rs = client.runQueryStatement(sqlcommand);
			String content = null;
			while(rs.next()){
				if(rs.getString(1).trim().equals("View Original Text:"))
					content = rs.getString(2);
			}
			if(content == null){
				throw new IllegalStateException("invalide view description");
			}
			Pattern pattern = Pattern.compile("(?i)\\b(?:exists|from|join)\\s+([a-zA-Z0-9_$#-]*\\.?\\s*(?:[a-zA-Z0-9_]+)*)");
			Matcher matcher = pattern.matcher(content);
			while(matcher.find()){
				String token[] = matcher.group().split("\\.");
				subStructures.add(token[1]);
			}
			rs.close();
			
			return subStructures;
		}  catch (SQLException e){
			client.close();
			throw new SQLException("Error: ", e);
		} catch (ArrayIndexOutOfBoundsException e){
			client.close();
			throw e;
		}
	}
	
	private static boolean validateStateView(View view, ImpalaJDBCClient client) throws IOException, SQLException{

		List<View> ViewSubViews = view.getSubViews();
		List<Table> ViewSubTables = view.getSubTables();
		List<String> subViewImpala = getViewSubEntity(client,view.getName());
		List<String> subViewContext = new ArrayList<String>();
		if(ViewSubViews != null)
		for(View v: ViewSubViews)
			subViewContext.add(v.getName());
		if(ViewSubTables != null)
		for(Table t:ViewSubTables){
			subViewContext.add(t.getName());
		}
		if(subViewImpala.containsAll(subViewContext))
			return true;
		else
			return false;
		
	}

	private static void validateStateImpala(CompactionContext context, ImpalaJDBCClient client) throws IOException, SQLException{

		Assert.assertTrue( validateStateView(context.getView(),client));
		Assert.assertTrue( validateStateView(context.getView1(),client));
		Assert.assertTrue( validateStateView(context.getView2(),client));
	}
	
	@Before
	public void setUp() throws ClassNotFoundException, IOException, SQLException{

		Assume.assumeTrue("unknown/unreachable impala host, skip test",isHostReachable(impalaHost));
		Assume.assumeTrue("hdfs not accessible, skip test",isPortOpen(impalaHost, impalaPort));
		String connectionUrl = "jdbc:impala://"+impalaHost+":"+impalaPort;
		String jdbcDriverName = "com.cloudera.impala.jdbc41.Driver";	
		client = new ImpalaJDBCClient(connectionUrl, jdbcDriverName);
		createTestTable(client);
		context = InitState.init(client, TEST_TABLE_NAME, TEST_TABLE_LOCATION, MANAGED_LOCATION, STATE_FILE);
		System.out.println("test environment initialized");
	}

	public void testSwitchLandingTable(CompactionContext context) throws SQLException, IOException{
		SwitchLandingTable.run(context);
		validateStateFile(context, States.StateII);
		validateStateImpala(context, client);
		System.out.println("testSwitchLandingTable finished");
	}

	public void testMoveDataFromLandingToPersist(CompactionContext context, ImpalaJDBCClient client) throws SQLException, IOException{
		MoveDataFromLandingToPersist.run(client, context);
		validateStateFile(context, States.StateIII);
		validateStateImpala(context, client);
		System.out.println("testMoveDataFromLandingToPersist finished");
	}

	public void testSwitchViewToTempTable(CompactionContext context, ImpalaJDBCClient client) throws SQLException, IOException{
		SwitchViewToTempTable.run(client, context);
		validateStateFile(context, States.StateIV);
		validateStateImpala(context, client);
		System.out.println("testSwitchViewToTempTable finished");
	}

	public void testRecreateOldLandingTable(CompactionContext context, ImpalaJDBCClient client) throws SQLException, IOException{
		RecreateOldLandingTable.run(client, context);
		validateStateFile(context, States.StateV);
		validateStateImpala(context, client);
		System.out.println("testRecreateOldLandingTable finished");
	}

	public void testAddRecreatedLandingTableToView(CompactionContext context, ImpalaJDBCClient client) throws SQLException, IOException{
		AddRecreatedLandingTableToView.run(client, context);
		validateStateFile(context, States.StateVI);
		validateStateImpala(context, client);
		System.out.println("testAddRecreatedLandingTableToView finished");
	}

	public void testSyncTwoPersistTable(CompactionContext context, ImpalaJDBCClient client) throws SQLException, IOException{
		SyncTwoPersistTable.run(client, context);
		validateStateFile(context, States.StateVII);
		validateStateImpala(context, client);
		System.out.println("testSyncTwoPersistTable finished");
	}

	public void testRemoveLandingTableFromView(CompactionContext context, ImpalaJDBCClient client) throws SQLException, IOException{
		RemoveLandingTableFromView.run(client, context);
		validateStateFile(context, States.StateI);
		validateStateImpala(context, client);
		System.out.println("testRemoveLandingTableFromView finished");
	}

	@Test
	public void compaction() throws SQLException, IOException{
		testSwitchLandingTable(context);
		testMoveDataFromLandingToPersist(context,client);
		testSwitchViewToTempTable(context,client);
		testRecreateOldLandingTable(context,client);
		testAddRecreatedLandingTableToView(context,client);
		testSyncTwoPersistTable(context,client);
		testRemoveLandingTableFromView(context,client);
	}


	@After
	public void clean() throws SQLException, IOException{
		
		System.out.println("test environment cleaned");
		if(client != null){
			drop();
			client.close();
		}
	}

	public void drop() throws SQLException{

		client.dropTable(context.getStore_table1().getName());
		client.dropTable(context.getStore_table2().getName());
		client.dropTable(context.getLandingTable1().getName());
		client.dropTable(context.getLandingTable2().getName());

		client.dropView(context.getView().getName());
		client.dropView(context.getView1().getName());
		client.dropView(context.getView2().getName());


	}

	public static void main(String args[]) throws ClassNotFoundException, IOException, SQLException{

		OperationTest test=new OperationTest();
		test.setUp();
		test.compaction();
		test.clean();
	}

}
