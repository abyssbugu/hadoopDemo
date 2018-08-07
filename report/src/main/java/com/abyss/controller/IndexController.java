package com.abyss.controller;


import com.abyss.service.AvgPvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;


@Controller
public class IndexController {
    
    
    @Autowired
    private AvgPvService pvSeivice;
    
    @RequestMapping("/")
    public String showIndex2() {
        return "index";
    }

    @RequestMapping("/index")
    public String showIndex() {
        return "index";
    }
    
    @RequestMapping(value="/avgPvNum",produces="application/json;charset=UTF-8")
    @ResponseBody
    public String getBarChart1() throws Exception{
        
            System.out.println("获取平均pv数据...");
            
            String s = pvSeivice.getAvgPvNumByDates("20130919", "20130925");
            
            return s;
            
    }

}
