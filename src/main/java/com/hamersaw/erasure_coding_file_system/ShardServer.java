package com.hamersaw.erasure_coding_file_system;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.security.MessageDigest;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.hamersaw.erasure_coding_file_system.message.ErrorMsg;
import com.hamersaw.erasure_coding_file_system.message.Message;
import com.hamersaw.erasure_coding_file_system.message.ShardServerHeartbeatMsg;
import com.hamersaw.erasure_coding_file_system.message.ReplyShardMsg;
import com.hamersaw.erasure_coding_file_system.message.ReplyShardServerMsg;
import com.hamersaw.erasure_coding_file_system.message.RequestShardMsg;
import com.hamersaw.erasure_coding_file_system.message.RequestShardServerMsg;

class ShardServer implements Runnable {
	private static final Logger LOGGER = Logger.getLogger(ShardServer.class.getCanonicalName());
	protected String storageDirectory, controllerHostName;
	protected int controllerPort, port;
	protected boolean stopped;
	protected Map<String,Map<Integer,List<Integer>>> shards, newShards;
	protected ServerSocket serverSocket;
	private ReadWriteLock readWriteLock;

	public ShardServer(String storageDirectory, String controllerHostName, int controllerPort, int port) {
		if(storageDirectory.endsWith(File.separator)) {
			this.storageDirectory = storageDirectory.substring(0, storageDirectory.length() - 2);
		} else {
			this.storageDirectory = storageDirectory;
		}

		this.controllerHostName = controllerHostName;
		this.controllerPort = controllerPort;
		this.port = port;
		stopped = false;
		shards = new HashMap<String,Map<Integer,List<Integer>>>();
		newShards = new HashMap<String,Map<Integer,List<Integer>>>();
		readWriteLock = new ReentrantReadWriteLock();
	}

	public static void main(String[] args) {
		try {
			String storageDirectory = args[0];
			String controllerHostName = args[1];
			int controllerPort = Integer.parseInt(args[2]);
			int port = Integer.parseInt(args[3]);

			new Thread(
				new ShardServer(storageDirectory, controllerHostName, controllerPort, port)
			).start();
		} catch(Exception e) {
			System.out.println("Usage: ShardServer storageDirectory controllerHostName controllerPort port");
			System.exit(1);
		}
	}

	@Override
	public void run() {
		try {
			serverSocket = new ServerSocket(port);
			LOGGER.info("Chunk server started successfully");

			//start HeartbeatTask that executes every 30 seconds
			Timer timer = new Timer();
			timer.schedule(new HeartbeatTask(), 0, 30 * 1000);

			while(!stopped) {
				Socket socket = serverSocket.accept();
				LOGGER.fine("Received connection from '" + socket.getInetAddress() + ":" + socket.getPort() + "'.");
			
				new Thread(new ShardServerWorker(socket, this)).start();
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public synchronized void writeShard(String filename, int chunkNum, int shardNum, int length, byte[] bytes, boolean eof, long timestamp) throws Exception{
		readWriteLock.writeLock().lock();
		try {
			//search for filename
			Map<Integer,List<Integer>> chunks;
			if(shards.containsKey(filename)) {
				chunks = shards.get(filename);
			} else {
				chunks = new HashMap<Integer,List<Integer>>();
				shards.put(filename, chunks);
			}

			//search for chunk number
			List<Integer> shardList;
			if(chunks.containsKey(chunkNum)) {
				shardList = chunks.get(chunkNum);
			} else {
				shardList = new LinkedList<Integer>();
				chunks.put(chunkNum, shardList);
			}

			//search for shard number
			int version = 1;
			if(shardList.contains(shardNum)) {
				//check timestamp in file and increase version number and rewrite if need be
				try {
					File file = new File(getFilename(storageDirectory, filename, chunkNum, shardNum));
					DataInputStream in = new DataInputStream(new FileInputStream(file));
	
					version = in.readInt();
					long shardTimestamp = in.readLong();
					in.close();

					if(shardTimestamp < timestamp) {
						version++;
						file.delete();
					} else {
						return;
					}
				} catch(Exception e) {
					LOGGER.severe("Unknown error writing shard '" + filename + ":" + chunkNum + ":" + shardNum + "'. '" + e.getMessage() + "'.");
					return;
				}
			} else {
				shardList.add(shardNum);
			}

			//add to newShards
			Map<Integer,List<Integer>> newChunks;
			if(newShards.containsKey(filename)) {
				newChunks = newShards.get(filename);
			} else {
				newChunks = new HashMap<Integer,List<Integer>>();
				newShards.put(filename, newChunks);
			}

			List<Integer> shardsList;
			if(newChunks.containsKey(chunkNum)) {
				shardsList = newChunks.get(chunkNum);
			} else {
				shardsList = new LinkedList<Integer>();
				newChunks.put(chunkNum, shardsList);
			}

			if(!shardsList.contains(shardNum)) {
				shardsList.add(shardNum);
			}

			//create new file if needed
			File file = new File(getFilename(storageDirectory, filename, chunkNum, shardNum));
			file.getParentFile().mkdirs();
			DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

			//write out metadata
			out.writeInt(version);
			out.writeLong(timestamp);
			out.writeInt(chunkNum);
			out.writeInt(shardNum);
			out.writeInt(length);
			out.writeBoolean(eof);

			//write bytes
			for(byte b : bytes) {
				out.writeByte(b);
			}

			LOGGER.info("Wrote shard '" + filename + ":" + chunkNum + ":" + shardNum + "' - length:" + length + " eof:" + eof + " timestamp:" + timestamp);
			out.close();
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	public synchronized Message requestShard(String filename, int chunkNum, int shardNum) throws Exception {
		readWriteLock.readLock().lock();
		try {
			if(!shards.containsKey(filename) || !shards.get(filename).containsKey(chunkNum) || !shards.get(filename).get(chunkNum).contains(shardNum)) {
				throw new Exception("Shard server doesn't contain shard '" + filename + ":" + chunkNum + ":" + shardNum + "'");
			}

			//read in shard
			File file = new File(getFilename(storageDirectory, filename, chunkNum, shardNum));
			if(!file.exists()) {
				throw new Exception("");
			}

			DataInputStream in = new DataInputStream(new FileInputStream(file));
			int version = in.readInt();
			long timestamp = in.readLong();
			int chunkNumber = in.readInt();
			int shardNumber = in.readInt();
			int length = in.readInt();
			boolean eof = in.readBoolean();

			byte[] bytes = new byte[Client.SHARD_SIZE];
			in.read(bytes);
			//TODO determine if there is data corruption
			/*if(in.read(bytes) != length) {
				throw new Exception("Unable to read " + length + " bytes from shard '" + filename + ":" + chunkNum + ":" + shardNum + "'");
			}*/

			in.close();
			return new ReplyShardMsg(filename, chunkNum, shardNum, length, bytes, eof, timestamp);
		} finally {
			readWriteLock.readLock().unlock();
		}	
	}

	public synchronized void stop() {
		stopped = true;
	}

	public boolean getStopped() {
		return stopped;
	}

	private String getFilename(String storageDirectory, String filename, int chunkNum, int shardNum) {
		if(filename.charAt(0) == File.separatorChar) {
			return storageDirectory + filename + "_chunk" + chunkNum + "_shard" + shardNum;
		} else {
			return storageDirectory + File.separatorChar + filename + "_chunk" + chunkNum + "_shard" + shardNum;
		}
	}

	class HeartbeatTask extends TimerTask {
		private int count;

		public HeartbeatTask() {
			count = 0;
		}

		@Override
		public void run() {
			try {
				//count total shards
				int totalShards = 0;
				for(Map<Integer,List<Integer>> map : shards.values()) {
					for(List<Integer> list : map.values()) {
						totalShards += list.size();
					}
				}

				//determine free space
				long freeSpace = Long.MAX_VALUE - totalShards;

				//create heartbeat message
				ShardServerHeartbeatMsg message = new ShardServerHeartbeatMsg(new ShardServerMetadata(serverSocket.getInetAddress(), serverSocket.getLocalPort(), totalShards, freeSpace));
				if(count % 8 == 0) { //8 * 30 seconds = major every 5 minutes
					LOGGER.fine("Executing major HeartbeatTask");
					message.setShards(shards);
				} else {
					LOGGER.fine("Executing minor HeartbeatTask");
					message.setShards(newShards);
				}

				//send heartbeat message
				Socket socket = new Socket(controllerHostName, controllerPort);
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(message);

				newShards.clear();
				socket.close();
			} catch(Exception e) {
				e.printStackTrace();
			}

			count++;
		}
	}
}
