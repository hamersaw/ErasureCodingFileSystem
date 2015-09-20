package com.hamersaw.erasure_coding_file_system;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.Socket;

import java.util.LinkedList;
import java.util.logging.Logger;

import com.hamersaw.erasure_coding_file_system.message.ControllerHeartbeatMsg;
import com.hamersaw.erasure_coding_file_system.message.ErrorMsg;
import com.hamersaw.erasure_coding_file_system.message.Message;
import com.hamersaw.erasure_coding_file_system.message.RequestShardMsg;
import com.hamersaw.erasure_coding_file_system.message.ReplyShardMsg;
import com.hamersaw.erasure_coding_file_system.message.SuccessMsg;
import com.hamersaw.erasure_coding_file_system.message.WriteShardMsg;

public class ShardServerWorker implements Runnable {
	private static Logger LOGGER = Logger.getLogger(ShardServerWorker.class.getCanonicalName());
	protected Socket socket;		
	protected ShardServer shardServer;

	public ShardServerWorker(Socket socket, ShardServer shardServer) {
		this.socket = socket;
		this.shardServer = shardServer;
	}

	@Override
	public void run() {
		try {
			//read request message
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			Message requestMsg = (Message) in.readObject();

			Message replyMsg = null;
			try {
				switch(requestMsg.getMsgType()) {
				case Message.CONTROLLER_HEARTBEAT_MSG:
					break;
				case Message.WRITE_SHARD_MSG:
					LOGGER.info("Received message of type 'WRITE_SHARD_MSG' from '" + socket.getInetAddress() + ":" + socket.getPort());
					WriteShardMsg wsMsg = (WriteShardMsg) requestMsg;
					shardServer.writeShard(wsMsg.getFilename(), wsMsg.getChunkNum(), wsMsg.getShardNum(), wsMsg.getLength(), wsMsg.getBytes(), wsMsg.getEof(), wsMsg.getTimestamp());

					break;
				case Message.REQUEST_SHARD_MSG:
					LOGGER.info("Received message of type 'REQUEST_SHARD_MSG' from '" + socket.getInetAddress() + ":" + socket.getPort());
					RequestShardMsg rsMsg = (RequestShardMsg) requestMsg;

					try {
						replyMsg = shardServer.requestShard(rsMsg.getFilename(), rsMsg.getChunkNum(), rsMsg.getShardNum());
					} catch(Exception e) {
						replyMsg = new ErrorMsg(e.getMessage());
					}

					break;
				default:
					LOGGER.severe("Unrecognized request message type '" + requestMsg.getMsgType() + "'");
					break;
				}
			} catch(Exception e) {
				LOGGER.severe(e.getMessage());
			}

			//write reply message
			if(replyMsg != null) {
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(replyMsg);
			}

			//close client socket
			socket.close();
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
