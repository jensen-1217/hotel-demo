package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch._types.LatLonGeoLocation;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchAllQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Highlight;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.NamedValue;
import net.bytebuddy.matcher.ClassLoaderHierarchyMatcher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.*;

/**
 * @author jensen
 * @date 2024-09-27 15:47
 * @description
 */
@SpringBootTest
public class TestSearch {
    @Autowired
    private ElasticsearchClient client;

    @Test
    public void test01() throws IOException {
        // 构建查询请求，MatchAllQuery 相当于查询所有
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index("hotel")
                .query(q -> q
                        .matchAll(MatchAllQuery.of(m -> m)))
        );
        // 执行查询
        SearchResponse<HotelDoc> response = client.search(searchRequest, HotelDoc.class);
        // 输出结果
        response.hits().hits().forEach(hit -> {
            System.out.println(hit.source());
        });
        System.out.println("一共" + response.hits().total().value());
    }

    @Test
    public void test02() throws IOException {
        // 构建查询请求，match 查询某个字段
//        SearchRequest searchRequest = SearchRequest.of(s -> s
//                .index("hotel")
//                .query(q -> q
//                        .match(m->m.field("all").query("如家")))
//        );
        // 构建查询请求，multiMatch 查询多个字段
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index("hotel")
                .query(q -> q
                        .multiMatch(m -> m.query("如家").fields("brand", "name")))
        );
        // 执行查询
        SearchResponse<HotelDoc> response = client.search(searchRequest, HotelDoc.class);
        // 输出结果
        response.hits().hits().forEach(hit -> {
            System.out.println(hit.source());
        });
        System.out.println("一共" + response.hits().total().value());
    }

    @Test
    public void test03() throws IOException {
//        // 构建查询请求，term 精确查询
//        SearchRequest searchRequest = SearchRequest.of(s -> s
//                .index("hotel")
//                .query(q -> q
//                        .term(m->m.field("city").value("杭州")))
//        );
        // 构建查询请求，range 范围查询
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index("hotel")
                .query(q -> q
                        .range(m -> m.field("price").gte(JsonData.of(100)).lte(JsonData.of(150))))
        );
        // 执行查询
        SearchResponse<HotelDoc> response = client.search(searchRequest, HotelDoc.class);
        // 输出结果
        response.hits().hits().forEach(hit -> {
            System.out.println(hit.source());
        });
        System.out.println("一共" + response.hits().total().value());
    }

    @Test
    public void test04() throws IOException {
        // 构建查询请求，range 范围查询
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index("hotel")
                .query(q -> q
                        .bool(b -> b
                                .must(m -> m.term(t -> t.field("city").value("杭州")))
                                .filter(f -> f.range(r -> r.field("price").lte(JsonData.of(250))))))
        );
        // 执行查询
        SearchResponse<HotelDoc> response = client.search(searchRequest, HotelDoc.class);
        // 输出结果
        response.hits().hits().forEach(hit -> {
            System.out.println(hit.source());
        });
        System.out.println("一共" + response.hits().total().value());
    }

    @Test
    public void test05() throws IOException {
        //使用 `geo_distance` 查询
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index("hotel")
                .query(q -> q
                        .geoDistance(g -> g
                                .field("location") // 假设地理坐标字段名为 location
                                .distance("5km") // 距离范围
                                .location(l->l
                                        .latlon(ll->ll
                                                .lon(121.492969)
                                                .lat(31.245409))
                                )
                        )
                )
        );
        // 使用 `geo_bounding_box` 查询
//        SearchRequest searchRequest = SearchRequest.of(s -> s
//                .index("hotel")
//                .query(q -> q
//                        .geoBoundingBox(g -> g
//                                .field("location")
//                                .boundingBox(b -> b
//                                        .tlbr(tb -> tb
//                                                .topLeft(tl -> tl
//                                                        .latlon(ll -> ll
//                                                                .lat(31.433423)
//                                                                .lon(121.082597)))
//                                                .bottomRight(br -> br
//                                                        .latlon(ll -> ll
//                                                                .lat(30.760561)
//                                                                .lon(121.735701)))
//                                        )))));
        // 执行查询
        SearchResponse<HotelDoc> response = client.search(searchRequest, HotelDoc.class);
        // 输出结果
        response.hits().hits().forEach(hit -> {
            System.out.println(hit.source());
        });
        System.out.println("一共" + response.hits().total().value());
    }

    @Test
    public void test06() throws IOException {
        // 构建查询请求，按价格升序排序
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index("hotel") // 查询的索引
                .query(q -> q
                        .matchAll(m -> m) // 查询所有文档
                )
                .sort(sort -> sort // 添加排序
                        .field(f -> f
                                .field("price") // 按照 price 字段排序
                                .order(SortOrder.Asc) // 升序排序
                        )
                )
        );
        // 执行查询
        SearchResponse<HotelDoc> response = client.search(searchRequest, HotelDoc.class);
        // 输出结果
        response.hits().hits().forEach(hit -> {
            System.out.println(hit.source());
        });
        System.out.println("一共" + response.hits().total().value());
    }

    @Test
    public void test07() throws IOException {
        // 构建查询请求，分页
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index("hotel")
                .query(q -> q
                        .matchAll(m -> m) // 查询所有文档
                )
                .from(10) // 跳过前 10 条记录
                .size(1) // 每页返回 10 条记录
                .sort(sort -> sort
                        .field(f -> f
                                .field("price")
                                .order(SortOrder.Asc)
                        )
                )
        );
        // 执行查询
        SearchResponse<HotelDoc> response = client.search(searchRequest, HotelDoc.class);
        // 输出结果
        response.hits().hits().forEach(hit -> {
            System.out.println(hit.source());
        });
        System.out.println("一共" + response.hits().total().value());
    }

    @Test
    public void test08() throws IOException {
        Map<String, HighlightField> highlightFieldMap = new HashMap<>();
        highlightFieldMap.put("name",new HighlightField.Builder().build());
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index("hotel")
                .query(q -> q
                        .match(m -> m.field("all").query("如家")) // 查询所有文档
                ).highlight(h->h.fields("name",new HighlightField.Builder().build()).requireFieldMatch(false))
                //.highlight(h->h.preTags("<em>").postTags("</em>").fields(highlightFieldMap))
                );
        // 执行查询
        SearchResponse<HotelDoc> response = client.search(searchRequest, HotelDoc.class);
        // 输出结果
        response.hits().hits().forEach(hit -> {
            // 根据字段名获取高亮结果的高亮值
            String name = hit.highlight().get("name").get(0);
            // 获取文档source
            HotelDoc source = hit.source();
            // 覆盖非高亮结果
            source.setName(name);
            System.out.println(source);
            List<FieldValue> sort = hit.sort();
        });
        System.out.println("一共" + response.hits().total().value());
    }

    @Test
    public void test09() throws IOException {
        // 构建查询请求，分页
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index("hotel")
                .size(0)
                .aggregations("brandAgg",a->a.
                        terms(h->h.
                                field("brand")
                                .size(20).order(Collections.singletonList(NamedValue.of("_count", SortOrder.Desc))))
        ));
        // 执行查询
        SearchResponse<HotelDoc> response = client.search(searchRequest, HotelDoc.class);
        // 输出结果
        Map<String, Aggregate> aggregations = response.aggregations();
        Aggregate brandAgg = aggregations.get("brandAgg");
        StringTermsAggregate brandAggs = brandAgg.sterms();
        List<StringTermsBucket> termsBucketList = brandAggs.buckets().array();
        ArrayList<String> buckets = new ArrayList<>();
        termsBucketList.forEach(bucket->{
            buckets.add(bucket.key().stringValue());
            System.out.println("key:"+bucket.key().stringValue());
            System.out.println("docCount：" + bucket.docCount());
        });
        System.out.println(buckets);
        response.hits().hits().forEach(hit -> {
            System.out.println(hit.source());
        });
        System.out.println("一共" + response.hits().total().value());
    }


}
