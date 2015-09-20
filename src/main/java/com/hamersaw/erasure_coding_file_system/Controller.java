package com.hamersaw.erasure_coding_file_system;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.hamersaw.erasure_coding_file_system.message.ControllerHeartbeatMsg;
import com.hamersaw.erasure_coding_file_system.message.ErrorMsg;
import com.hamersaw.erasure_coding_file_system.message.Message;
import com.hamersaw.erasure_coding_file_system.message.RequestShardServerMsg;
import com.hamersaw.erasure_coding_file_system.message.ReplyShardServerMsg;

public class Controller implements Runnable {
	private static final Logger LOGGER = Logger.getLogger(Controller.class.getCanonicalName());
	protected int port;
	protected boolean stopped;
	protected ServerSocket serverSocket;
	protected List<ShardServerMetadata> shardServers;
	protected Map<String,Map<Integer,Map<Integer,ShardServerMetadata>>> shards;

	public Controller(int port) {
		this.port = port;
		stopped = false;
		shardServers = new LinkedList<ShardServerMetadata>();
		shards = new HashMap<String,Map<Integer,Map<Integer,ShardServerMetadata>>>();
	}

	public static void main(String[] args) {
		try {
			int port = Integer.parseInt(args[0]);

			new Thread(new Controller(port)).start();
		} catch(Exception e) {
			System.out.println("Usage: Controller port replicationCount");
			System.exit(1);
		}
	}

	@Override
	public void run() {
		try{
			ServerSocket serverSocket = new ServerSocket(port);
			LOGGER.info("Controller started successfully");
			
			//start HeartbeatTask that executes every 30 seconds
			Timer timer = new Timer();
			timer.schedule(new HeartbeatTask(), 0, 30 * 1000);

			while(!stopped) {
				Socket socket = serverSocket.accept();
				LOGGER.fine("Received connection from '" + socket.getInetAddress() + ":" + socket.getPort() + "'.");

				new Thread(
					new ControllerWorker(
						socket,
						this
					)
				).start();
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public synchronized void updateShardServer(ShardServerMetadata shardServerMetadata, Map<String,Map<Integer,List<Integer>>> shardServerShards) {
		//search for shard server
		boolean found = false;
		for(ShardServerMetadata shardServer : shardServers) {
			if(shardServer.compareTo(shardServerMetadata) == 0) {
				shardServer.setTotalChunks(shardServerMetadata.getTotalChunks());
				shardServer.setFreeSpace(shardServerMetadata.getFreeSpace());
				shardServerMetadata = shardServer;
				found = true;
				break;
			}
		}

		if(!found) {
			LOGGER.info("Adding ShardServer '" + shardServerMetadata + "'");
			shardServers.add(shardServerMetadata);
		}

		//update shards
		for(String filename : shardServerShards.keySet()) {
			for(int chunkNum : shardServerShards.get(filename).keySet()) {
				for(int shardNum : shardServerShards.get(filename).get(chunkNum)) {
					//search for filename
					Map<Integer,Map<Integer,ShardServerMetadata>> chunkNums;
					if(shards.containsKey(filename)) {
						chunkNums = shards.get(filename);
					} else {
						chunkNums = new HashMap<Integer,Map<Integer,ShardServerMetadata>>();
						shards.put(filename, chunkNums);
					}

					//search for chunk number
					Map<Integer,ShardServerMetadata> shards;
					if(chunkNums.containsKey(chunkNum)) {
						shards = chunkNums.get(chunkNum);
					} else {
						shards = new HashMap<Integer,ShardServerMetadata>();
						chunkNums.put(chunkNum, shards);
					}

					//search for shard number
					if(shards.containsKey(shardNum)) {
						if(!shards.get(shardNum).equals(shardServerMetadata)) {
							LOGGER.severe("Attempting to register a duplication of shard '" + filename + ":" + chunkNum + ":" + shardNum + ")");
						}
					} else {
						LOGGER.info("Registering shard '" + filename + ":" + chunkNum + ":" + shardNum + "' to server '" + shardServerMetadata.getInetAddress() + ":" + shardServerMetadata.getPort() + "'");
						shards.put(shardNum, shardServerMetadata);
					}
				}
			}
		}
	}

	public ShardServerMetadata requestShardServer(String filename, int chunkNum, int shardNum,  boolean writeOperation) throws Exception{
		ShardServerMetadata shardServerMetadata;
		if(shards.containsKey(filename) && shards.get(filename).containsKey(chunkNum) && shards.get(filename).get(chunkNum).containsKey(shardNum)) {
			shardServerMetadata  = shards.get(filename).get(chunkNum).get(shardNum);
		} else if(!writeOperation) {
			throw new Exception("Shard '" + filename + ":" + chunkNum + ":" + shardNum + "' not found in any shard servers.");
		} else if(shardServers.size() == 0) {
			throw new Exception("No shard servers have been registered yet.");
		} else {
			shardServerMetadata = shardServers.get(0);
			long freeSpace = shardServerMetadata.getFreeSpace();
			for(int i=1; i<shardServers.size(); i++) {
				if(shardServers.get(i).getFreeSpace() > freeSpace) {
					shardServerMetadata = shardServers.get(i);
					freeSpace = shardServerMetadata.getFreeSpace();
				}
			}
		}

		//update free space if write operation
		if(writeOperation) {
			shardServerMetadata.setFreeSpace(shardServerMetadata.getFreeSpace() - 1);
		}
		
		return shardServerMetadata;
	}

	public synchronized void stop() {
		stopped = true;
	}

	public boolean getStopped() {
		return stopped;
	}

	class HeartbeatTask extends TimerTask {
		@Override
		public void run() {
			List<ShardServerMetadata> unreachableList = new LinkedList<ShardServerMetadata>();
			for(ShardServerMetadata shardServerMetadata : shardServers) {
				try {
					//open a socket
					Socket socket = new Socket(shardServerMetadata.getInetAddress(), shardServerMetadata.getPort());

					//send a heartbeat message
					ControllerHeartbeatMsg message = new ControllerHeartbeatMsg();
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(message);
				} catch(Exception e) {
					unreachableList.add(shardServerMetadata);
					LOGGER.info("Remove shard server '" + shardServerMetadata + "'. Host unreachable.");
				}
			}

			//remove unreachable chunk servers
			for(ShardServerMetadata shardServerMetadata : unreachableList) {
				shardServers.remove(shardServerMetadata);

				//remove shards
				List<Integer> shardRemoveList = new LinkedList<Integer>();
				for(String filename : shards.keySet()) {
					for(int chunkNum : shards.get(filename).keySet()) {
						Map<Integer,ShardServerMetadata> map = shards.get(filename).get(chunkNum);

						for(int shardNum : map.keySet()) {
							if(map.get(shardNum).equals(shardServerMetadata)) {
								shardRemoveList.add(shardNum);
							}
						}

						for(int shardNum : shardRemoveList) {
							map.remove(shardNum);
						}

						shardRemoveList.clear();
					}
				}
			}
		}
	}
}
