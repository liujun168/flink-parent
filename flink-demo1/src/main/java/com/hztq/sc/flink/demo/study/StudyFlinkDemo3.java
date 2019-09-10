package com.hztq.sc.flink.demo.study;

import com.hztq.sc.flink.demo.db.JDBCUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @description: flinkDemo3
 * @author: liujun 249489478@qq.com
 * @create: 2019-09-06 17:48
 */
public class StudyFlinkDemo3 {

    public static void main(String[] args) {
        Connection conn = JDBCUtils.getConnection();
        String sql="select ORGID,YEAR,TOTAL,VISIT from RS_YP_RENTALHOUSE_VISIT where rownum<10";
        List<Map<Object, Object>> query = JDBCUtils.query(sql, conn);
        List<VisitVO> vos = new ArrayList<>();
        query.forEach(data->{
            VisitVO vo = new VisitVO();
            vo.setOrgId((BigDecimal) data.get("orgId"));
            vo.setYear((BigDecimal) data.get("year"));
            vo.setTotal((BigDecimal) data.get("total"));
            vo.setVisit((BigDecimal) data.get("visit"));
            vos.add(vo);
//            System.out.println(data.get("orgId")+"   "+data.get("year")+"  "+data.get("total")+"  "+data.get("visit"));
        });
        System.out.println(vos);
//        DataSet<> dataSet
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    }

}

class VisitVO{

    private BigDecimal orgId;
    private BigDecimal year;
    private BigDecimal total;
    private BigDecimal visit;

    public BigDecimal getOrgId() {
        return orgId;
    }

    public void setOrgId(BigDecimal orgId) {
        this.orgId = orgId;
    }

    public BigDecimal getYear() {
        return year;
    }

    public void setYear(BigDecimal year) {
        this.year = year;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public void setTotal(BigDecimal total) {
        this.total = total;
    }

    public BigDecimal getVisit() {
        return visit;
    }

    public void setVisit(BigDecimal visit) {
        this.visit = visit;
    }

    @Override
    public String toString() {
        return "VisitVO{" +
                "orgId=" + orgId +
                ", year=" + year +
                ", total=" + total +
                ", visit=" + visit +
                '}';
    }
}