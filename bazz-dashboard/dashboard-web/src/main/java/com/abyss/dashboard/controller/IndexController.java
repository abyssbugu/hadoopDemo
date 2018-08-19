package com.abyss.dashboard.controller;

import com.abyss.dashboard.service.DAUService;
import com.abyss.dashboard.service.OrderService;
import com.abyss.dashboard.service.UserService;
import com.abyss.dashboard.utils.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Controller
public class IndexController {

    @Autowired
    private DAUService dauService;

    @Autowired
    private UserService userService;
    @Autowired
    private OrderService orderService;


    @RequestMapping("index")
    public ModelAndView index(){
        ModelAndView mv = new ModelAndView("index");

        // 今天
        mv.addObject("dau_day", this.dauService.queryCountByDate(0));
        // 昨天
        mv.addObject("dau_day_before", this.dauService.queryCountByDate(-1));
        // 上月这一天
        mv.addObject("dau_day_month", this.dauService.queryCountByDate(-30));
        // 今天格式化字符串
        mv.addObject("dau_day_str", DateUtils.formatDate(2));
        mv.addObject("dau_day_before_str", DateUtils.formatDate(DateUtils.dateAddDays(new Date(),-1)));
        mv.addObject("dau_day_month_str", DateUtils.getDateStrOfDayLastMonth());

        return mv;
    }
    @GetMapping("payOrder")
    @ResponseBody
    public Map<String,Object> payOrder(){
        return this.orderService.queryPayOrderGroupByDate();
    }

    @GetMapping("userCity")
    @ResponseBody
    public List<Map<String, Object>> userCity(){
        return this.userService.queryUserCityGroupByCity();
    }
}
