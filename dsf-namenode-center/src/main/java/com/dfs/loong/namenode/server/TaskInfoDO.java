package com.dfs.loong.namenode.server;


import java.util.Date;

/**
 * <p>
 *
 * </p>
 *
 * @author sgl
 * @since 2020-03-31
 */
public class TaskInfoDO {

    /**
     * 任务ID
     */
    private Long taskId;

    /**
     * 任务标题
     */
    private String taskTitle;

    /**
     * 任务内容描述
     */
    private String taskDescription;

    /**
     * 系统来源
     */
    private String taskSystemSource;

    /**
     * 业务流程唯一标识
     */
    private String taskBusinessId;

    /**
     * 业务任务唯一标识
     */
    private String taskRequestId;

    /**
     * 业务类型代码
     */
    private String taskFlDm;

    /**
     * 任务状态:已办01、待办02、无效10
     */
    private String taskStatus;

    /**
     * 任务状态说明
     */
    private String taskStatusExplain;

    /**
     * 任务流程图
     */
    private String taskFlowchartAddr;

    /**
     * 任务发起人
     */
    private String taskBusinessCreator;

    /**
     * 任务办结期限
     */
    private Date taskDeadline;

    /**
     * 总办结时间
     */
    private Date businessDeadline;

    /**
     * 任务对象类型：个人待办03、岗位待办04
     */
    private String blDxlx;

    /**
     * 任务对象代码：个人待办时传税务人员代码，岗位待办传岗位序号
     */
    private String blDxDm;


    /**
     * 任务办理地址
     */
    private String blUrl;

    /**
     * 任务对象名称
     */
    private String blDxmc;

    /**
     * 任务办理机关代码
     */
    private String blSwjgDm;

    /**
     * 任务办理机关名称
     */
    private String blSwjgmc;

    /**
     * 任务办理岗位代码
     */
    private String blGwDm;

    /**
     * 任务办理岗位名称
     */
    private String blGwmc;



    /**
     * 任务创建时间
     */
    private Date createTime;
    /**
     * 任务发送时间
     */
    private Date sendTime;
    /**
     * 任务接收时间
     */
    private Date receiveTime;

    /**
     * 任务完成时间
     */
    private Date completionTime;

    /**
     * 岗位序号
     */
    private String gwxh;

    /**
     * 流程名称
     */
    private String activityName;


    /**
     * 是否已读
     */
    private Integer readStatus = 0;


    /**
     * 任务来源人代码
     */
    private String lySwryDm;

    /**
     * 任务来源人名称
     */
    private String lySwrymc;

    /**
     * 任务来源岗位代码
     */
    private String lyGwDm;

    /**
     * 任务来源岗位名称
     */
    private String lyGwmc;

    /**
     * 任务来源税务机关代码
     */
    private String lySwjgDm;

    /**
     * 任务来源税务机关名称
     */
    private String lySwjgmc;








    /**
     * 请求唯一标识
     */
    private String taskKey;







    /**
     * 业务串联序列号
     */
    private String requestId;

    /**
     * 数据归属地区
     */
    private String sjgsdq;

    /**
     * 数据修改地区
     */
    private String sjxgdq;


    private String lrrDm;

    private String xgrDm;

    private Date lrrq;

    private Date xgrq;



    private String yxbz = "Y";

    /**
     * 优先级
     */
    private Integer priority = 0;
    /**
     * 提醒日期(YYYY-MM-DD字符格式)
     */
    private String remindDay;
    /**
     * 任务标签，多个的话以,隔开
     */
    private String labelId;
}

