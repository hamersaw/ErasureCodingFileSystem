package com.hamersaw.erasure_coding_file_system.message;

public class ReplyShardMsg extends Message {
	private String filename;
	private int chunkNum, shardNum, length;
	private byte[] bytes;
	private boolean eof;
	private long timestamp;

	public ReplyShardMsg(String filename, int chunkNum, int shardNum, int length, byte[] bytes, boolean eof, long timestamp) {
		this.filename = filename;
		this.chunkNum = chunkNum;
		this.shardNum = shardNum;
		this.length = length;
		this.bytes = bytes;
		this.eof = eof;
		this.timestamp = timestamp;
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

	public int getLength() {
		return length;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public boolean getEof() {
		return eof;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public int getMsgType() {
		return REPLY_SHARD_MSG;
	}
}
