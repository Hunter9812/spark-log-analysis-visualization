package com.example.controller;

import com.example.entity.NameValue;
import com.example.service.PublisherService;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Resource
    private PublisherService publisherService;

    @GetMapping("dauRealtime")
    public Map<String, Object> dauRealtime(@RequestParam("td") String td) {
        return publisherService.doDauRealtime(td);
    }

    @GetMapping("statsByItem")
    public List<NameValue> statsByItem(@RequestParam("itemName") String itemName,
                                       @RequestParam("date") String date,
                                       @RequestParam("t") String t) {
        return publisherService.doStatsByItem(itemName, date, t);
    }

    @GetMapping("detailByItem")
    public Map<String, Object> detailByItem(@RequestParam("date") String date,
                                       @RequestParam("itemName") String itemName,
                                       @RequestParam(value = "pageNo", required = false, defaultValue = "1") Integer pageNo,
                                       @RequestParam(value = "pageSize", required = false, defaultValue = "20") Integer pageSize) {
        return publisherService.doDetailByItem(date, itemName, pageNo, pageSize);
    }

//    @GetMapping("visitTotal")
//    public
}
