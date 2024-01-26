package com.flinkuse.cases.cqcal.entity;

import com.flinkuse.core.enums.SqlOperate;

/**
 * @author learn
 * @date 2023/5/18 11:05
 */
public class CqEsEntity {

    private SqlOperate action;
    private Long watermark;
    private Object data;
    private String index;
    private String _id;

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

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

