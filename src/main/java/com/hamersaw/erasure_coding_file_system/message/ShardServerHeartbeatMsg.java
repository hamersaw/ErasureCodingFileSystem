package com.hamersaw.erasure_coding_file_system.message;

import java.net.InetAddress;

import java.util.List;
import java.util.Map;

import com.hamersaw.erasure_coding_file_system.ShardServerMetadata;

public class ShardServerHeartbeatMsg extends Message {
	private ShardServerMetadata shardServerMetadata;
	private Map<String,Map<Integer,List<Integer>>> shards;	

	public ShardServerHeartbeatMsg(ShardServerMetadata shardServerMetadata) {
		this.shardServerMetadata = shardServerMetadata;
	}

	public ShardServerMetadata getShardServerMetadata() {
		return shardServerMetadata;
	}

	public void setShards(Map<String,Map<Integer,List<Integer>>> shards) {
		this.shards = shards;
	}

	public Map<String,Map<Integer,List<Integer>>> getShards() {
		return shards;
	}

	@Override
	public int getMsgType() {
		return SHARD_SERVER_HEARTBEAT_MSG;
	}
}
