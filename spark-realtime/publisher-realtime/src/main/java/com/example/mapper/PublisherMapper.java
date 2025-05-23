package com.example.mapper;

import com.example.entity.NameValue;

import java.util.List;
import java.util.Map;

public interface PublisherMapper {
    Map<String, Object> searchDau(String td);

    List<NameValue> searchStatsByItem(String itemName, String date, String field);

    Map<String, Object> searchDetailByItem(String date, String itemName, Integer from, Integer pageSize);

}
