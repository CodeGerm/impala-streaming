package org.cg.impala.streaming;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

public class HdfsClient {
	
	
	private Configuration conf;
	private DFSClient dfs;
	public static String HDFS_CONNECTION_NAME = "fs.defaultFS";
	
	public HdfsClient(String connection) throws IOException {
		conf = new Configuration();
		conf.set(HDFS_CONNECTION_NAME, connection);
		dfs = new DFSClient(NameNode.getAddress(conf),conf);
	}
	
	public Boolean checkDir(String path) throws IOException{
		
		if(dfs.getFileInfo(path)==null)
			return false;
		return dfs.getFileInfo(path).isDir();
	}
	
	public void mkDir(String path) throws IOException{
		FsPermission fp = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL);
		
		dfs.mkdirs(path, fp, true);
	}
	
	public void setOwner(String path, String name, String groupName) throws IOException{
		dfs.setOwner(path, name, groupName);
		
	}
	
	public void close() throws IOException{
		dfs.close();
	}
	
	public static void main(String args[]) throws IOException{
		
		
		//System.setProperty("HADOOP_USER_NAME", "hdfs");
		HdfsClient client = new HdfsClient("hdfs://192.168.99.100:8020");
		
		
		System.out.println(client.checkDir("/user/impala"));
		
		client.mkDir("/user/test");
		
	}
	

}
