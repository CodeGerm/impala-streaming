package org.cg.impala.streaming.compaction;

public class CompactionStatus {
	
	String compactionId;
	String landingTable;
	String tableState;
	String tableName;
	Status status;
	String failedReason;
	
	public static enum Status {
		running, finished, failed
	}
	public String getCompactionId() {
		return compactionId;
	}
	public void setCompactionId(String compactionId) {
		this.compactionId = compactionId;
	}
	public String getLandingTable() {
		return landingTable;
	}
	public void setLandingTable(String landingTable) {
		this.landingTable = landingTable;
	}
	public String getTableState() {
		return tableState;
	}
	public void setTableState(String tableState) {
		this.tableState = tableState;
	}
	public Status getStatus() {
		return status;
	}
	public void setStatus(Status status) {
		this.status = status;
	}
	
	public String getFailedReason() {
		return failedReason;
	}
	public void setFailedReason(String failedReason) {
		this.failedReason = failedReason;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public CompactionStatus(String compactionId, String landingTable, String tableState, String tableName,
			Status status) {
		super();
		this.compactionId = compactionId;
		this.landingTable = landingTable;
		this.tableState = tableState;
		this.tableName = tableName;
		this.status = status;
	}
	@Override
	public String toString() {
		return "CompactionStatus [compactionId=" + compactionId + ", landingTable=" + landingTable + ", tableState="
				+ tableState + ", tableName=" + tableName + ", status=" + status + ", failedReason=" + failedReason
				+ "]";
	}
	
	
}
