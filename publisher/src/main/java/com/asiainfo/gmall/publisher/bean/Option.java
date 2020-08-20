package com.asiainfo.gmall.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 封装共有属性，男女比例和年龄比例，都是 名称和比例两个字段
 * 自动增加 getter 和setter 方法
 * 增加所以属性的构造方法
 * 一个对象放一条数据，最后组成的就是一个list集合
 */
@Data
@AllArgsConstructor
public class Option {
    private String name;
    private  double rate;

}
