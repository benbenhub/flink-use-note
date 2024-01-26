package com.flinkuse.cases.adapter.entity;

/**
 * es索引category_query_cmoemstd实体类
 * @author learn
 * @date 2023/5/18 11:21
 */
public class CategoryQueryCmoemstd {
    private String cm_id;
    private String create_time;
    private String oem_carmodel_remark_id;
    private String oem_partsku_id;
    private String std_partsku_id;
    private String std_tree_id;
    private String watermark;

    public String getWatermark() {
        return watermark;
    }

    public void setWatermark(String watermark) {
        this.watermark = watermark;
    }

    public String getCm_id() {
        return cm_id;
    }

    public void setCm_id(String cm_id) {
        this.cm_id = cm_id;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getOem_carmodel_remark_id() {
        return oem_carmodel_remark_id;
    }

    public void setOem_carmodel_remark_id(String oem_carmodel_remark_id) {
        this.oem_carmodel_remark_id = oem_carmodel_remark_id;
    }

    public String getOem_partsku_id() {
        return oem_partsku_id;
    }

    public void setOem_partsku_id(String oem_partsku_id) {
        this.oem_partsku_id = oem_partsku_id;
    }

    public String getStd_partsku_id() {
        return std_partsku_id;
    }

    public void setStd_partsku_id(String std_partsku_id) {
        this.std_partsku_id = std_partsku_id;
    }

    public String getStd_tree_id() {
        return std_tree_id;
    }

    public void setStd_tree_id(String std_tree_id) {
        this.std_tree_id = std_tree_id;
    }
}
