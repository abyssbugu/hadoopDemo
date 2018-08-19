package com.abyss.dashboard.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * Created by Abyss on 2018/8/19.
 * description:
 */
public interface OrderMapper {

    /**
     * 根据时间范围查询支付订单的数据并且按照日期分组
     *
     * @param start
     * @param end
     * @return
     */
    List<Map<String, Object>> queryPayOrderGroupByDate(@Param("start") String start, @Param("end") String end);
}

