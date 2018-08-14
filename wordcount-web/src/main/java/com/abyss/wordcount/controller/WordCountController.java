package com.abyss.wordcount.controller;

import com.abyss.wordcount.service.WordCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

/**
 * Created by Abyss on 2018/8/14.
 * description:
 */
@Controller
public class WordCountController {


    @Autowired
    private WordCountService wordCountService;

    @RequestMapping("view")
    public String wordCountView() {
        return "view";
    }

    @RequestMapping("data")
    @ResponseBody
    public Map<String, Integer> queryData() {
        return this.wordCountService.queryData();
    }


}
