package com.hamersaw.erasure_coding_file_system.message;

import com.hamersaw.erasure_coding_file_system.ShardServerMetadata;

public class ReplyShardServerMsg extends Message {
	private ShardServerMetadata shardServer;

	public ReplyShardServerMsg(ShardServerMetadata shardServer) {
		this.shardServer = shardServer;
	}

	public ShardServerMetadata getShardServer() {
		return shardServer;
	}

	@Override
	public int getMsgType() {
		return REPLY_SHARD_SERVER_MSG;
	}
}
