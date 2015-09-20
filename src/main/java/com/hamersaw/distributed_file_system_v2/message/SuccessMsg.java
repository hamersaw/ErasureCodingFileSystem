package com.hamersaw.distributed_file_system_v2.message;

public class SuccessMsg extends Message {
	public int getMsgType() {
		return SUCCESS_MSG;
	}
}
