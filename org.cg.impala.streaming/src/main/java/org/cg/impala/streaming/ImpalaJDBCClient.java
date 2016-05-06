package org.cg.impala.streaming;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ImpalaJDBCClient {



	private Connection con;

	private static final Log logger = LogFactory.getLog(ImpalaJDBCClient.class);


	public ImpalaJDBCClient(String connectionUrl, String jdbcDriverName) throws IOException, ClassNotFoundException, SQLException {
		init(connectionUrl, jdbcDriverName);
	}



	private void init(String connectionUrl,String jdbcDriverName) throws IOException, ClassNotFoundException, SQLException {

		Class.forName(jdbcDriverName);
		
		con = DriverManager.getConnection(connectionUrl);

	}

	public boolean isPartitioned(String tableName) throws SQLException{
		ResultSet rs = null;
		Statement stmt = null;
		try{
			stmt = con.createStatement();
			String sqlStatement = "describe formatted "+tableName;
			rs = stmt.executeQuery(sqlStatement);
			while(rs.next()){
				if(rs.getString(1).trim().equals("# Partition Information")){
					return true;
				}
			}
			return false;
		} catch (SQLException e){
			throw e;
		} finally {
			if(rs!=null)
				rs.close();
			if(stmt!=null)
				stmt.close();
		}
	}

	public ResultSet runQueryStatement(String sqlStatement) throws SQLException{
		logger.info("running query statement: "+sqlStatement);
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery(sqlStatement);
		} catch (SQLException e){
			throw e;
		} finally {
			if(stmt!=null)
				stmt.close();
		}
		return rs;
	}

	public void runUpdateStatement(String sqlStatement) throws SQLException{
		logger.info("running update statement: "+sqlStatement);
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			stmt.setQueryTimeout(100);
			stmt.executeUpdate(sqlStatement);
		} catch (SQLException e){
			throw e;
		} finally {
			if(stmt!=null)
				stmt.close();
		}
	}

	public void recoverPartition(String tableName) throws SQLException{
		if(isPartitioned(tableName)){
			String sqlStatement = "alter table "+tableName+" recover partitions";
			runUpdateStatement(sqlStatement);
		} else {
			logger.info("table is not partitioned, use refresh instead");
			refresh(tableName);
		}

	}

	public String getTableLocation(String tableName) throws SQLException{
		ResultSet rs = null;
		String location = null;
		Statement stmt = null;
		try{
			stmt = con.createStatement();
			String sqlStatement = "describe formatted "+tableName;
			rs = stmt.executeQuery(sqlStatement);
			while(rs.next()){
				if(rs.getString(1).trim().equals("Location:"))
					location = rs.getString(2).trim();
			}
			rs.close();
		} catch (SQLException e){
			if(rs != null)
				rs.close();
			throw e;
		} finally {
			if(rs!=null)
				rs.close();
			if(stmt!=null)
				stmt.close();
		}
		return location;
	}


	public List<String> getPartitionedColumn(String tableName) throws SQLException{
		List<String> columns = new ArrayList<String>();
		ResultSet rs = null;
		Statement stmt = null;
		try{
			stmt = con.createStatement();
			boolean searchingIndicator = false;
			String sqlStatement = "describe formatted "+tableName;
			rs = stmt.executeQuery(sqlStatement);
			while(rs.next()){
				String firstCol = rs.getString(1);
				if(firstCol.trim().equals("# Partition Information"))
					searchingIndicator = true;
				else if(searchingIndicator){
					if(firstCol.trim().equals("# Detailed Table Information"))
						searchingIndicator = false;
					else if(!firstCol.isEmpty()&&!firstCol.contains("#")){
						columns.add(firstCol.trim());	
					}
				}
			}
		} catch (SQLException e){
			throw e;
		} finally {
			if(rs!=null)
				rs.close();
			if(stmt!=null)
				stmt.close();
		}
		return columns;
	}

	public void refresh(String tableName) throws SQLException{
		String sqlStatement = "refresh "+tableName;
		runUpdateStatement(sqlStatement);
	}

	public void invalidate(String tableName) throws SQLException{
		String sqlStatement = "invalidate metadata "+tableName;
		runUpdateStatement(sqlStatement);
	}

	public void dropView(String viewName) throws SQLException{
		String sqlStatement = "drop view if exists "+viewName;
		runUpdateStatement(sqlStatement);

	}

	public void createView(String viewName, List<String> subEntity) throws SQLException{
		String subEntityString = "";
		for(int i=0;i<subEntity.size();i++){
			subEntityString+=" select * from "+subEntity.get(i);
			if(i<subEntity.size()-1)
				subEntityString+=" union all ";
		}
		String sqlStatement = "create view if not exists "+viewName+" as "+subEntityString;
		runUpdateStatement(sqlStatement);
	}

	public void alterView(String viewName, List<String> subEntity) throws SQLException{
		String subEntityString = "";
		for(int i=0;i<subEntity.size();i++){
			subEntityString+=" select * from "+subEntity.get(i);
			if(i<subEntity.size()-1)
				subEntityString+=" union all ";
		}
		String sqlStatement = "alter view "+viewName+" as "+subEntityString;
		runUpdateStatement(sqlStatement);
	}

	public void dropTable(String tableName) throws SQLException{
		String sqlStatement = "drop table if exists "+tableName;
		runUpdateStatement(sqlStatement);
	}

	public void createLandingTable(String tableName, String referenceTableName, String location) throws SQLException{
		String sqlStatement = "create table if not exists "+tableName+" like "+referenceTableName+" stored as avro";
		if(location != null)
			sqlStatement += " location '"+location+"'";
		runUpdateStatement(sqlStatement);
	}

	public void createStoringTable(String tableName, String referenceTableName, String location) throws SQLException{
		String sqlStatement = "create table if not exists "+tableName+" like "+referenceTableName+" stored as parquet";
		if(location != null)
			sqlStatement += " location '"+location+"'";
		runUpdateStatement(sqlStatement);
	}

	public void compaction(String tmpTable, String persistTable) throws SQLException{
		String sqlStatement = null;
		if (isPartitioned(persistTable)){
			//Here we assume that the tmp table has the same partitions as the persist table
			List<String> cols = getPartitionedColumn(persistTable);
			String partitionCols = "partition(";
			for(int i=0; i<cols.size() ; i++){
				if(i == cols.size()-1)
					partitionCols += cols.get(i)+")";
				else{
					partitionCols += cols.get(i)+", ";
				}
			}
			sqlStatement = "insert into "+persistTable+" "+partitionCols+" [shuffle] "+"select * from "+tmpTable;
		}
		else
			sqlStatement = "insert into "+persistTable+" select * from "+tmpTable;
		runUpdateStatement(sqlStatement);
	}


	//Use Cloudera recommended COMPUTE STATS for now 
	public void updateStats(String tableName) throws SQLException{
		String sqlStatement = " compute incremental stats "+tableName;
		runUpdateStatement(sqlStatement);
	}



	public void close() throws SQLException{
		if(con!=null)
			con.close();
	}



}
