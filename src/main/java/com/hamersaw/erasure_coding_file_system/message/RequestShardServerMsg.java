package com.hamersaw.erasure_coding_file_system.message;

public class RequestShardServerMsg extends Message {
	private String filename;
	private int chunkNum, shardNum;
	private boolean writeOperation;

	public RequestShardServerMsg(String filename, int chunkNum, int shardNum, boolean writeOperation) {
		this.filename = filename;
		this.chunkNum = chunkNum;
		this.shardNum = shardNum;
		this.writeOperation = writeOperation;
	}

	public String getFilename() {
		return filename;
	}

	public int getChunkNum() {
		return chunkNum;
	}

	public int getShardNum() {
		return shardNum;
	}

	public boolean getWriteOperation() {
		return writeOperation;
	}

	@Override
	public int getMsgType() {
		return REQUEST_SHARD_SERVER_MSG;
	}
}
