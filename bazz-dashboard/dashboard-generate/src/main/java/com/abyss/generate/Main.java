package com.abyss.generate;

import com.abyss.generate.service.IGenerate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Map;

public class Main {

    public static void main(String[] args) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath*:application-*.xml");
        Map<String, IGenerate> map = applicationContext.getBeansOfType(IGenerate.class);
        for (Map.Entry<String, IGenerate> entry : map.entrySet()) {
            entry.getValue().start();
        }
    }
}
