package com.flinkuse.cases.common;

import com.flinkuse.core.enums.SqlOperate;

/**
 * @author learn
 * @date 2023/5/18 11:05
 */
public class AdapterEsEntity {

    private SqlOperate action;
    private Long watermark;
    private Object data;

    public Long getWatermark() {
        return watermark;
    }

    public void setWatermark(Long watermark) {
        this.watermark = watermark;
    }

    public SqlOperate getAction() {
        return action;
    }

    public void setAction(SqlOperate action) {
        this.action = action;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}

