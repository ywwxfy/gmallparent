package com.asiainfo.gmall.publisher.service.impl;

import com.asiainfo.gmall.common.constants.GmallConstants;
import com.asiainfo.gmall.publisher.mapper.DauMapper;
import com.asiainfo.gmall.publisher.mapper.OrderMapper;
import com.asiainfo.gmall.publisher.service.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.queryparser.xml.QueryBuilderFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rescore.RescoreBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class DauServiceImpl implements DauService {
    @Autowired
    DauMapper dauMapper;
    @Autowired
    OrderMapper orderMapper;
    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String dayString) {
        return dauMapper.getCount(dayString);
    }

    //{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
    //"today":{"12":38,"13":1233,"17":123,"19":688 }}
    @Override
    public Map<String,Long> getDauHourTotal(String dayString) {
        Map<String, Long> map = new HashMap<>();
        List<Map> hourTotalCount = dauMapper.getHourTotalCount(dayString);
        for (Map m : hourTotalCount) {
            String hourKey = (String)m.get("LH");
            Long hourvalue= (Long) m.get("CT");
            map.put(hourKey, hourvalue);
        }
        return map;
    }

    @Override
    public Double getOrderAmountTotal(String dayString) {
        return orderMapper.getOrderAmountTotal(dayString);
    }

    @Override
    public Map<String, Double> getOrderAmountHourTotal(String dayString) {
        Map<String, Double> hashmap = new HashMap<>();
        List<Map> orderAmountHourTotalMap = orderMapper.getOrderAmountHourTotal(dayString);
        for (Map map : orderAmountHourTotalMap) {
            //利用entry set得到map中 key 和value的值
//            Set<Map.Entry<String,Double>> set = map.entrySet();
//            for (Map.Entry<String, Double> entry : set) {
//                String key = entry.getKey();
//                Double value = entry.getValue();
//            }
            //直接对map进行遍历
            System.out.println(map.toString());
//            map.iter
            String newKey = (String)map.get("CH");
            Double newValue = (Double) map.get("TM");
//            map.forEach((key, value) -> {
            //
//                System.out.println(key+" ="+value);
//                hashmap.put((String) key, (Double) value);

//            });
            hashmap.put(newKey, newValue);
        }
//        System.out.println(hashmap.toString());
        return hashmap;
    }

    /**
     * 根据给定的时间日期字段和关键词，从es中获取过滤后的信息
     * 分页展示
     * 关键词是要进行分词的
     * 不能写sql,写dsl语句
     * 最终结果 两个饼图 年龄聚合比例，性别聚合比例
     *          一个明细
     *          用一个map来存放这些结果
     *
     *
     * @param dayString
     * @param skuKeyword
     * @return
     */
    @Override
    public Map getSaleDetail(String dayString, String skuKeyword,int pageSize,int pageNum) {
//        "hits" : [
//        {
//            "_index" : "gmall2019_sale_detail",
//                "_type" : "_doc",
//                "_id" : "3911",
//                "_score" : 2.144402,
//                "_source" : {
//            "order_detail_id" : "3911",
//                    "order_id" : "1415",
//                    "order_status" : "1",
//                    "create_time" : "2020-08-15 00:29:29",
//                    "user_id" : "171",
//                    "sku_id" : "1",
//                    "user_age" : 0,
//                    "sku_price" : 2220.0,
//                    "sku_name" : "荣耀10青春版 幻彩渐变 2400万AI自拍 全网通版4GB+64GB 渐变蓝 移动联通电信4G全面屏手机 双卡双待",
//                    "dt" : "2020-08-15"
//        }
//        },...] 页面查询出来的是一个数组，里面的数据可以用map对象封装，然后用list包含
        //groupby 也是一个map的形式，多个key,key是数字
        //第二种结果存放方式 : 明细用一个对象来封装，对象也会放到集合里面才行

        //构造查询sql
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //bool query 得到关键词和根据时间过滤的结果
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder dtTerm = new TermQueryBuilder("dt", dayString);
         boolQueryBuilder.filter(dtTerm);
        //分词操作
        MatchQueryBuilder sku_nameMatch = new MatchQueryBuilder("sku_name", skuKeyword);
        sku_nameMatch.operator(MatchQueryBuilder.Operator.AND);

          boolQueryBuilder.must(sku_nameMatch);
          //searchSource 把boolQuery添加进去
         searchSourceBuilder.query(boolQueryBuilder);
        TermsBuilder user_gender = AggregationBuilders.terms(GmallConstants.ES_QUERY_AGGREGATION_GENDER).field("user_gender").size(2);
        TermsBuilder user_age = AggregationBuilders.terms(GmallConstants.ES_QUERY_AGGREGATION_USERAGE).field("user_age").size(100);
        //聚合操作
        searchSourceBuilder.aggregation(user_gender);
        searchSourceBuilder.aggregation(user_age);
        //分页操作
        searchSourceBuilder.from((pageNum - 1) * pageSize);
        searchSourceBuilder.size(pageSize);

//        SearchSourceBuilder query1 = searchBuilder.query(query);
//        searchBuild
        Map resultMap = new HashMap();
        String queryString = searchSourceBuilder.toString();
        System.out.println(queryString);
        Search searchBuilder = new Search.Builder(queryString).addIndex(GmallConstants.ES_INDEX_SALE_DETAIL).addType("_doc").build();
        try {
            SearchResult result = jestClient.execute(searchBuilder);
            Long total = result.getTotal();
            //把总数放到map中
            resultMap.put("total", total);
            //得到hits ,最后我们想要得到的是_source 下面的内容,销售明细
            List<SearchResult.Hit<Map, Void>> hitsMap = result.getHits(Map.class);
            ArrayList<Map> hitList = new ArrayList<>();
            for (SearchResult.Hit<Map, Void> hit : hitsMap) {
                hitList.add(hit.source);
            }
            resultMap.put("saleDetail", hitList);
            MetricAggregation aggregations = result.getAggregations();
            //性别聚合
            Map<String, Long> genderMap = new HashMap<>();
            List<TermsAggregation.Entry> user_genderEntry = aggregations.getTermsAggregation(GmallConstants.ES_QUERY_AGGREGATION_GENDER).getBuckets();
            for (TermsAggregation.Entry entry : user_genderEntry) {
                genderMap.put(entry.getKey(), entry.getCount());

            }
            resultMap.put("genderMap", genderMap);
            Map<String,Long> ageMap=new HashMap<>();

            List<TermsAggregation.Entry> user_ageBuckets = aggregations.getTermsAggregation(GmallConstants.ES_QUERY_AGGREGATION_USERAGE).getBuckets();
            for (TermsAggregation.Entry user_ageBucket : user_ageBuckets) {
                ageMap.put(user_ageBucket.getKey(), user_ageBucket.getCount());
            }
            resultMap.put("ageMap", ageMap);
        } catch (IOException e) {
            e.printStackTrace();
        }


        return resultMap;
    }
}
