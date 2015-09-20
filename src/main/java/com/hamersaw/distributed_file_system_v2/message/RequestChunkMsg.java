package com.hamersaw.distributed_file_system_v2.message;

import java.net.InetAddress;

public class RequestChunkMsg extends Message {
	private String filename;
	private int chunkNum;

	public RequestChunkMsg(String filename, int chunkNum) {
		this.filename = filename;
		this.chunkNum = chunkNum;
	}

	public String getFilename() {
		return filename;
	}

	public int getChunkNum() {
		return chunkNum;
	}

	@Override
	public int getMsgType() {
		return REQUEST_CHUNK_MSG;
	}
}
