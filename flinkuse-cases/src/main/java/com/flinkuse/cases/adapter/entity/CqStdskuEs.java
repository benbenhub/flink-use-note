package com.flinkuse.cases.adapter.entity;

import java.util.List;

/**
 * es索引category_query_stdsku实体类
 * @author learn
 * @date 2023/5/18 11:12
 */
public class CqStdskuEs {
    private String category_id;
    private String create_time;
    private String ten_partsku_id;
    private String ten_brand_id;
    private String ten_category_id;
    private String watermark;
    private String tenant_id;
    private List<Rel> rel;

    public String getTenant_id() {
        return tenant_id;
    }

    public void setTenant_id(String tenant_id) {
        this.tenant_id = tenant_id;
    }

    public String getWatermark() {
        return watermark;
    }

    public void setWatermark(String watermark) {
        this.watermark = watermark;
    }

    public String getCategory_id() {
        return category_id;
    }

    public void setCategory_id(String category_id) {
        this.category_id = category_id;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getTen_partsku_id() {
        return ten_partsku_id;
    }

    public void setTen_partsku_id(String ten_partsku_id) {
        this.ten_partsku_id = ten_partsku_id;
    }

    public String getTen_brand_id() {
        return ten_brand_id;
    }

    public void setTen_brand_id(String ten_brand_id) {
        this.ten_brand_id = ten_brand_id;
    }

    public String getTen_category_id() {
        return ten_category_id;
    }

    public void setTen_category_id(String ten_category_id) {
        this.ten_category_id = ten_category_id;
    }

    public List<Rel> getRel() {
        return rel;
    }

    public void setRel(List<Rel> rel) {
        this.rel = rel;
    }

    public static class Rel {
        private String std_partsku_id;
        private String std_tree_id;
        private String oem_partsku_id;
        private String cm_id;
        private String ten_cm_comment;
        private String oem_carmodel_remark_id;

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

        public String getOem_partsku_id() {
            return oem_partsku_id;
        }

        public void setOem_partsku_id(String oem_partsku_id) {
            this.oem_partsku_id = oem_partsku_id;
        }

        public String getCm_id() {
            return cm_id;
        }

        public void setCm_id(String cm_id) {
            this.cm_id = cm_id;
        }

        public String getTen_cm_comment() {
            return ten_cm_comment;
        }

        public void setTen_cm_comment(String ten_cm_comment) {
            this.ten_cm_comment = ten_cm_comment;
        }

        public String getOem_catmodel_remark_id() {
            return oem_carmodel_remark_id;
        }

        public void setOem_catmodel_remark_id(String oem_catmodel_remark_id) {
            this.oem_carmodel_remark_id = oem_catmodel_remark_id;
        }

        @Override
        public String toString() {
            return "Rel{" +
                    "std_partsku_id='" + std_partsku_id + '\'' +
                    ", std_tree_id='" + std_tree_id + '\'' +
                    ", oem_partsku_id='" + oem_partsku_id + '\'' +
                    ", cm_id='" + cm_id + '\'' +
                    ", ten_cm_comment='" + ten_cm_comment + '\'' +
                    ", oem_carmodel_remark_id='" + oem_carmodel_remark_id + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "CqStdskuEs{" +
                "category_id='" + category_id + '\'' +
                ", create_time='" + create_time + '\'' +
                ", ten_partsku_id='" + ten_partsku_id + '\'' +
                ", ten_brand_id='" + ten_brand_id + '\'' +
                ", ten_category_id='" + ten_category_id + '\'' +
                ", watermark='" + watermark + '\'' +
                ", tenant_id='" + tenant_id + '\'' +
                ", rel=" + rel +
                '}';
    }
}
