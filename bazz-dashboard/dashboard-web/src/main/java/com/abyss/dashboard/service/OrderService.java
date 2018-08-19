package com.abyss.dashboard.service;

import com.abyss.dashboard.mapper.OrderMapper;
import com.abyss.dashboard.utils.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Created by Abyss on 2018/8/19.
 * description:
 */
@Service
public class OrderService {

    @Autowired
    private OrderMapper orderMapper;

    public Map<String, Object> queryPayOrderGroupByDate() {
        Date date = new Date();
        String start = DateUtils.getDateBegin(DateUtils.formatDate(DateUtils.dateAddDays(date, -6)));
        String end = DateUtils.getDateBegin(DateUtils.formatDate(date));

        List<Map<String, Object>> list = this.orderMapper.queryPayOrderGroupByDate(start, end);
        Map<String, Long> data = new LinkedHashMap<>();
        for (Map<String, Object> map : list) {
            data.put((String) map.get("date"), (Long) map.get("num"));
        }

        Map<String, Object> result = new HashMap<>();
        result.put("x", new ArrayList<>());
        result.put("y", new ArrayList<>());
        for (int i = 6; i >= 0; i--) {
            List x = (List) result.get("x");
            List y = (List) result.get("y");
            Date d = DateUtils.dateAddDays(date, -i);
            x.add(DateUtils.formatDate(d, "MM/dd"));

            String dStr = DateUtils.formatDate(d);
            Long num = data.get(dStr);
            if (num == null) {
                y.add("0");
            } else {
                y.add(String.valueOf(num));
            }
        }

        return result;
    }

}
