package com.hamersaw.erasure_coding_file_system.message;

import java.io.Serializable;

public abstract class Message implements Serializable {
	public static final int ERROR_MSG = 0,
		REQUEST_SHARD_SERVER_MSG = 1,
		REPLY_SHARD_SERVER_MSG = 2,
		SHARD_SERVER_HEARTBEAT_MSG = 3,
		CONTROLLER_HEARTBEAT_MSG = 4,
		WRITE_SHARD_MSG = 5,
		REQUEST_SHARD_MSG = 6,
		REPLY_SHARD_MSG = 7,
		SUCCESS_MSG = 8;

	public abstract int getMsgType();
}
