package com.abyss.dashboard.service;

import com.abyss.dashboard.mapper.DAUMapper;
import com.abyss.dashboard.utils.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
public class DAUService {

    @Autowired
    private DAUMapper dauMapper;

    public Integer queryCountByDate(int type) {
        String start = null;
        String end = null;
        Date date = new Date();
        if (type == -1) {
            date = DateUtils.dateAddDays(date, -1);
        } else if (type == -30) {
            // 计算上月的这一天
            try {
                date = DateUtils.parseDate(DateUtils.getDateStrOfDayLastMonth(), DateUtils.PATTERN_THREE);
            } catch (Exception e) {
                e.printStackTrace();
                return -1;
            }
        }

        start = DateUtils.getDateBegin(DateUtils.formatDate(date));
        end = DateUtils.getDateEnd(DateUtils.formatDate(date));

        return this.dauMapper.queryCountByDate(start, end);
    }

}