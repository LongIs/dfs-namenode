package com.dfs.loong.namenode.server;

import lombok.Data;

/**
 * 代表了一条edits log
 * @author zhonghuashishan
 *
 */
@Data
public class EditLog {

	long txid;
	String content;

	public EditLog(long txid, String content) {
		this.txid = txid;
		this.content = content;
	}

}