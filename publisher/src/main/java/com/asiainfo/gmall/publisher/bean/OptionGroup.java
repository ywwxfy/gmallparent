package com.asiainfo.gmall.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/*
把两个饼图放到一个bean对象中
 */
@Data
@AllArgsConstructor
public class OptionGroup {
    private String title;
    private List<Option> list;

}
