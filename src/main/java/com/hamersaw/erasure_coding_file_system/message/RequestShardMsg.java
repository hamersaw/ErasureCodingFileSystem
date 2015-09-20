package com.hamersaw.erasure_coding_file_system.message;

import java.net.InetAddress;

public class RequestShardMsg extends Message {
	private String filename;
	private int chunkNum, shardNum;

	public RequestShardMsg(String filename, int chunkNum, int shardNum) {
		this.filename = filename;
		this.chunkNum = chunkNum;
		this.shardNum = shardNum;
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

	@Override
	public int getMsgType() {
		return REQUEST_SHARD_MSG;
	}
}
