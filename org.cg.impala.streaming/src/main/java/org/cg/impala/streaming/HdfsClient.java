package org.cg.impala.streaming;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

public class HdfsClient {
	
	
	private Configuration conf;
	private DFSClient dfs;
	public static String HDFS_CONNECTION_NAME = "fs.defaultFS";
	
	public HdfsClient(String connection) throws IOException {
		conf = new Configuration();
		conf.set(HDFS_CONNECTION_NAME, connection);
		conf.set("dfs.client.use.datanode.hostname", "false");
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
	
	public static String getUrlFromPath(String path) throws IOException{
		Path dfsPath = new Path(path);
		return dfsPath.getFileSystem(new Configuration()).getUri().toString();
		
	}
	
	public void setOwner(String path, String name, String groupName) throws IOException{
		dfs.setOwner(path, name, groupName);
		
	}
	
	public String[] listFiles(String dir) throws IOException{
		ArrayList<String> list =new ArrayList<String>();
		for(HdfsFileStatus status : dfs.listPaths(dir, HdfsFileStatus.EMPTY_NAME).getPartialListing()){
			if(!status.getLocalName().startsWith(".")&&!status.isDir())
				list.add(status.getLocalName());
		}
		return list.toArray(new String[list.size()]);
	}
	
	public void writeFile(String path, String content) throws IOException{
		OutputStream out = dfs.create(path, true);
		Writer outputStreamWriter = new OutputStreamWriter(out);
		outputStreamWriter.write(content);
		outputStreamWriter.close();
	}
	
	public String readFile(String path) throws IOException{
		DFSInputStream input = dfs.open(path);
		return getStringFromInputStream(input);
	}
	
	public void close() throws IOException{
		dfs.close();
	}
	
	private static String getStringFromInputStream(InputStream is) {

		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();

		String line;
		try {

			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return sb.toString();
	}

	
	public static void main(String args[]) throws IOException{
		
		
		System.setProperty("HADOOP_USER_NAME", "cloudera");
	
		HdfsClient client = new HdfsClient("hdfs://192.168.99.100:8020");
		client.writeFile("/user/cloudera/test.txt", "do what");
		//System.out.println(client.checkDir("/user/impala"));
		client.mkDir("/user/test");
		client.close();
	}
	

}
