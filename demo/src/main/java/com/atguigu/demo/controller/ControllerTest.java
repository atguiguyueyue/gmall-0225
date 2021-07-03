package com.atguigu.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller
//@RestController=@Controller+@ResponseBody
@RestController
public class ControllerTest {


    @RequestMapping("test")
//    @ResponseBody
    public String test1(){
        System.out.println("123");
        return "success";
    }

    @RequestMapping("test1")
//    @ResponseBody
    private String test2(@RequestParam("name") String na, @RequestParam("age") Integer ag){
        System.out.println("aaaaa");
        return "name:" + na + "age:" + ag;
    }

}
