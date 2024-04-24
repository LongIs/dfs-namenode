package com.dfs.loong.namenode.vo;

import com.alibaba.fastjson.JSONObject;
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

		JSONObject jsonObject = JSONObject.parseObject(content);
		jsonObject.put("txid", txid);

		this.content = jsonObject.toJSONString();
	}

	public String get(String key) {
		JSONObject jsonObject = JSONObject.parseObject(content);
		return jsonObject.getString(key);
	}

}