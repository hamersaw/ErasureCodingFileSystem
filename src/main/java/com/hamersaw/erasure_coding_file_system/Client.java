package com.hamersaw.erasure_coding_file_system;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.lang.NumberFormatException;

import java.net.Socket;

import java.util.List;
import java.util.logging.Logger;
import java.util.Scanner;

import com.backblaze.erasure.ReedSolomon;

import com.hamersaw.erasure_coding_file_system.message.ErrorMsg;
import com.hamersaw.erasure_coding_file_system.message.Message;
import com.hamersaw.erasure_coding_file_system.message.RequestShardMsg;
import com.hamersaw.erasure_coding_file_system.message.RequestShardServerMsg;
import com.hamersaw.erasure_coding_file_system.message.ReplyShardMsg;
import com.hamersaw.erasure_coding_file_system.message.ReplyShardServerMsg;
import com.hamersaw.erasure_coding_file_system.message.WriteShardMsg;

public class Client {
	private static final Logger LOGGER = Logger.getLogger(Client.class.getCanonicalName());
	public static final int CHUNK_SIZE = 60000, DATA_SHARDS = 6, PARITY_SHARDS = 3, TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS, SHARD_SIZE = CHUNK_SIZE / DATA_SHARDS;

	public static void main(String[] args) {
		String controllerHostName = null;
		int controllerPort = 0;

		try {
			controllerHostName = args[0];
			controllerPort = Integer.parseInt(args[1]);
		} catch(Exception e) {
			System.out.println("Usage: Client controllerHosName controllerPort");
			System.exit(1);
		}

		try {
			//start input loop
			String input = "";
			Scanner scanner = new Scanner(System.in);
			while(!input.equalsIgnoreCase("q")) {
				System.out.print("Options\nS) Store File\nR) Retrieve File\nQ) Quit\nInput:");
				input = scanner.nextLine();

				if(input.equalsIgnoreCase("s")) {
					//read in filename
					String filename;
					FileInputStream in;
					long timestamp, fileLength;
					try {
						System.out.print("\tFilename:");
						filename = scanner.nextLine();
						File file = new File(filename);
						timestamp = file.lastModified();
						fileLength = file.length();
						in = new FileInputStream(file);
					} catch(FileNotFoundException e) {
						System.out.println("File not found.");
						continue;
					} catch(Exception e) {
						e.printStackTrace();
						continue;
					}

					//loop through file
					byte[] chunkBytes = new byte[CHUNK_SIZE];
					int chunkLength, chunkNum = 0;
					while((chunkLength = in.read(chunkBytes)) != -1) {
						//figure out if this is the end of the file
						boolean eof = (CHUNK_SIZE * chunkNum + chunkLength == fileLength);

						//fill shards
						byte[][] shardBytes = new byte[TOTAL_SHARDS][SHARD_SIZE];
						for(int i=0; i<DATA_SHARDS; i++) {
							System.arraycopy(chunkBytes, i*SHARD_SIZE, shardBytes[i], 0, SHARD_SIZE);
						}

						//encode shards
						ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
						reedSolomon.encodeParity(shardBytes, 0, SHARD_SIZE);

						//loop over shards and write to each ShardServer
						for(int shardNum=0; shardNum<shardBytes.length; shardNum++) {
							//write shard server request message
							Socket socket = new Socket(controllerHostName, controllerPort);
							ObjectOutputStream socketOut = new ObjectOutputStream(socket.getOutputStream());
							RequestShardServerMsg rcsMsg = new RequestShardServerMsg(filename, chunkNum, shardNum, true);
							socketOut.writeObject(rcsMsg);

							//read shard server reply message
							ObjectInputStream socketIn = new ObjectInputStream(socket.getInputStream());
							Message replyMsg = (Message) socketIn.readObject();
							socket.close();

							if(replyMsg.getMsgType() == Message.ERROR_MSG) {
								LOGGER.severe(((ErrorMsg)replyMsg).getMsg());
								return;
							} else if(replyMsg.getMsgType() != Message.REPLY_SHARD_SERVER_MSG) {
								LOGGER.severe("Unexpected message type returned. Was expecting '" + Message.REPLY_SHARD_SERVER_MSG + "' and recieved '" + replyMsg.getMsgType() + "'");
								return;
							}

							//send write shard message
							ShardServerMetadata shardServerMetadata = ((ReplyShardServerMsg) replyMsg).getShardServer();
							WriteShardMsg wsMsg = new WriteShardMsg(
								filename,
								chunkNum,
								shardNum,
								chunkLength,
								shardBytes[shardNum],
								eof,
								timestamp
							);

							Socket shardServerSocket = new Socket(shardServerMetadata.getInetAddress(), shardServerMetadata.getPort());
							ObjectOutputStream shardServerOut = new ObjectOutputStream(shardServerSocket.getOutputStream());
							shardServerOut.writeObject(wsMsg);
							shardServerSocket.close();
						}

						//clear chunkBytes
						for(int i=0; i<chunkBytes.length; i++) {
							chunkBytes[i] = 0;
						}

						chunkNum++;
					}

					in.close();
				} else if(input.equalsIgnoreCase("r")) {
					//read in filename
					String filename;
					FileInputStream in;
					FileOutputStream out;
					try {
						System.out.print("\tFilename:");
						filename = scanner.nextLine();

						System.out.print("\tOutputDirectory:");
						String directory = scanner.nextLine();
						
						File file;
						if(directory.endsWith(File.separator)) {
							if(filename.startsWith(File.separator)) {
								file = new File(directory.substring(0, directory.length() - 2));
							} else {
								file = new File(directory + filename);
							}
						} else {
							if(filename.startsWith(File.separator)) {
								file = new File(directory + filename);
							} else {
								file = new File(directory + File.separator + filename);
							}
						}

						file.getParentFile().mkdirs();
						out = new FileOutputStream(file);
					} catch(FileNotFoundException e) {
						System.out.println("File not found.");
						continue;
					} catch(Exception e) {
						e.printStackTrace();
						continue;
					}

					boolean eof = false;
					int length = 0, chunkNum = 0;
					byte[][] shardBytes = new byte[TOTAL_SHARDS][];
					boolean[] shardPresent = new boolean[TOTAL_SHARDS];
					while(!eof) {
						int shardCount = 0;
						for(int shardNum=0; shardNum<shardBytes.length; shardNum++) {
							//write request shard servers
							Socket socket = new Socket(controllerHostName, controllerPort);
							ObjectOutputStream socketOut = new ObjectOutputStream(socket.getOutputStream());
							RequestShardServerMsg rssMsg = new RequestShardServerMsg(filename, chunkNum, shardNum, false);
							socketOut.writeObject(rssMsg);

							//read request chunk servers
							ObjectInputStream socketIn = new ObjectInputStream(socket.getInputStream());
							Message replyMsg = (Message) socketIn.readObject();
							socket.close();

							if(replyMsg.getMsgType() == Message.ERROR_MSG) {
								LOGGER.severe(((ErrorMsg)replyMsg).getMsg());
								shardBytes[shardNum] = new byte[SHARD_SIZE];
								shardPresent[shardNum] = false;
								continue;
							} else if(replyMsg.getMsgType() != Message.REPLY_SHARD_SERVER_MSG) {
								LOGGER.severe("Unexpected message type returned. Was expecting '" + Message.REPLY_SHARD_SERVER_MSG + "' and recieved '" + replyMsg.getMsgType() + "'");
								return;
							}

							//request chunks from chunk servers
							ShardServerMetadata shardServerMetadata = ((ReplyShardServerMsg) replyMsg).getShardServer();
							RequestShardMsg rsMsg = new RequestShardMsg(filename, chunkNum, shardNum);
							Socket clientSocket = new Socket(shardServerMetadata.getInetAddress(), shardServerMetadata.getPort());
							ObjectOutputStream clientOut = new ObjectOutputStream(clientSocket.getOutputStream());
							clientOut.writeObject(rsMsg);

							ObjectInputStream clientIn = new ObjectInputStream(clientSocket.getInputStream());
							replyMsg = (Message) clientIn.readObject();
							clientSocket.close();

							if(replyMsg.getMsgType() == Message.ERROR_MSG) {
								LOGGER.severe(((ErrorMsg)replyMsg).getMsg());
								shardBytes[shardNum] = new byte[SHARD_SIZE];
								shardPresent[shardNum] = false;
								continue;
							} else if(replyMsg.getMsgType() != Message.REPLY_SHARD_MSG) {
								LOGGER.severe("Unexpected message type returned. Was expecting '" + Message.REPLY_SHARD_MSG + "' and recieved '" + replyMsg.getMsgType() + "'");
								continue;
							}

							//parse reply message
							ReplyShardMsg replysMsg = (ReplyShardMsg) replyMsg;
							shardBytes[shardNum] = replysMsg.getBytes();
							eof = replysMsg.getEof();
							length = replysMsg.getLength();

							shardPresent[shardNum] = true;
							shardCount++;
						}

						if(shardCount < DATA_SHARDS) {
							LOGGER.severe("Unable to reconstruct chunk '" + filename + ":" + chunkNum + "'. " + shardCount + " shards were found and " + DATA_SHARDS + " are needed.");
							return;
						}

						//reconstruct chunk from erasure closure bytes
						ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
						reedSolomon.decodeMissing(shardBytes, shardPresent, 0, SHARD_SIZE);

						//write chunk out to file
						for(int i=0; i<shardBytes.length; i++) {
							for(int j=0; j<shardBytes[i].length && j<length-(i*SHARD_SIZE); j++) {
								out.write(shardBytes[i][j]);
							}
						}

						chunkNum++;
					}

					out.close();
				} else if(!input.equalsIgnoreCase("q")) {
					System.out.println("Unknown input '" + input  + "' please reenter.");
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
