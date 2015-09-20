package com.hamersaw.distributed_file_system_v2.message;

import java.net.InetAddress;

public class ForwardChunkMsg extends Message {
	private String filename;
	private int chunkNum, port;
	private InetAddress inetAddress;

	public ForwardChunkMsg(String filename, int chunkNum, InetAddress inetAddress, int port) {
		this.filename = filename;
		this.chunkNum = chunkNum;
		this.inetAddress = inetAddress;
		this.port = port;
	}

	public String getFilename() {
		return filename;
	}

	public int getChunkNum() {
		return chunkNum;
	}

	public void setInetAddress(InetAddress inetAddress) {
		this.inetAddress = inetAddress;
	}

	public InetAddress getInetAddress() {
		return inetAddress;
	}

	public int getPort() {
		return port;
	}

	@Override
	public int getMsgType() {
		return FORWARD_CHUNK_MSG;
	}
}
