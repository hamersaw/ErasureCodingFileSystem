package com.hamersaw.distributed_file_system_v2.message;

import java.util.List;

import com.hamersaw.distributed_file_system_v2.ChunkServerMetadata;

public class ReplyChunkServerMsg extends Message {
	private List<ChunkServerMetadata> chunkServers;

	public ReplyChunkServerMsg(List<ChunkServerMetadata> chunkServers) {
		this.chunkServers = chunkServers;
	}

	public List<ChunkServerMetadata> getChunkServers() {
		return chunkServers;
	}

	@Override
	public int getMsgType() {
		return REPLY_CHUNK_SERVER_MSG;
	}
}
