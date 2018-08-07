package com.abyss.controller;

import com.abyss.service.FlowService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;


@Controller
public class FlowController {
    
    @Autowired
    private FlowService flowService;
    
    @RequestMapping(value="/flowNum",produces="application/json;charset=UTF-8")
    @ResponseBody
    public String getBarChart1() throws Exception{
        
            String string = flowService.getFlowByDates("20131001", "20131007");
            
            return string;
            
    }

}
