package com.hamersaw.erasure_coding_file_system;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.Socket;

import java.util.List;
import java.util.logging.Logger;

import com.hamersaw.erasure_coding_file_system.message.ErrorMsg;
import com.hamersaw.erasure_coding_file_system.message.ShardServerHeartbeatMsg;
import com.hamersaw.erasure_coding_file_system.message.Message;
import com.hamersaw.erasure_coding_file_system.message.RequestShardServerMsg;
import com.hamersaw.erasure_coding_file_system.message.ReplyShardServerMsg;

public class ControllerWorker implements Runnable {
	private static Logger LOGGER = Logger.getLogger(ControllerWorker.class.getCanonicalName());
	protected Socket socket;		
	protected Controller controller;

	public ControllerWorker(Socket socket, Controller controller) {
		this.socket = socket;
		this.controller = controller;
	}

	@Override
	public void run() {
		try {
			//read request message
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			Message requestMsg = (Message) in.readObject();

			LOGGER.fine("Received message of type '" + requestMsg.getMsgType() + "' from '" + socket.getInetAddress() + ":" + socket.getPort());

			Message replyMsg = null;
			try {
				switch(requestMsg.getMsgType()) {
				case Message.REQUEST_SHARD_SERVER_MSG:
					LOGGER.info("Received message of type 'REQUEST_SHARD_SERVER_MSG' from '" + socket.getInetAddress() + ":" + socket.getPort());
					RequestShardServerMsg rcsMsg = (RequestShardServerMsg) requestMsg;

					try {
						replyMsg = new ReplyShardServerMsg(controller.requestShardServer(rcsMsg.getFilename(), rcsMsg.getChunkNum(), rcsMsg.getShardNum(), rcsMsg.getWriteOperation()));
					} catch(Exception e) {
						replyMsg = new ErrorMsg(e.getMessage());
					}

					break;
				case Message.SHARD_SERVER_HEARTBEAT_MSG:
					ShardServerHeartbeatMsg sshMsg = (ShardServerHeartbeatMsg) requestMsg;
					sshMsg.getShardServerMetadata().setInetAddress(socket.getInetAddress()); //TODO fix this up
					controller.updateShardServer(sshMsg.getShardServerMetadata(), sshMsg.getShards());
					break;
				default:
					LOGGER.severe("Unrecognized request message type '" + requestMsg.getMsgType() + "'");
					break;
				}
			} catch(Exception e) {
				e.printStackTrace();
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
