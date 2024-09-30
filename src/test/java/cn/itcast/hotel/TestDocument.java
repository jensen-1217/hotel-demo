package cn.itcast.hotel;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.impl.HotelService;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import com.alibaba.fastjson.JSON;
import org.elasticsearch.client.RequestOptions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jensen
 * @date 2024-09-26 2:14
 * @description
 */
@SpringBootTest
public class TestDocument {
    @Autowired
    private ElasticsearchClient client;
    @Autowired
    private HotelMapper hotelMapper;
    @Autowired
    private HotelService hotelService;

    @Test
    void testAddDocument() throws IOException {
        // 1.根据id查询酒店数据
        Hotel hotel = hotelService.getById(61083L);
        // 2.转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);
        // 3.将HotelDoc转json
//        String json = JSON.toJSONString(hotelDoc);
// 1.准备Request对象
        IndexResponse response = client.index(i -> i.index("hotel").document(hotelDoc).id("1"));
        System.out.println("response.result() = " + response.result());
        System.out.println("response.id() = " + response.id());
        System.out.println("response.seqNo() = " + response.seqNo());
        System.out.println("response.index() = " + response.index());
        System.out.println("response.shards() = " + response.shards());
    }

    @Test
    void testGetDocument() throws IOException {
        //构造查询条件
        SearchRequest searchRequest = SearchRequest.of(s -> s.index("hotel")
                .query(q -> q.match(m -> m.field("id").query("61083")))
                .query(q -> q.term(t -> t.field("name").value("皇冠")))
        );

        //处理响应结果
        SearchResponse<HotelDoc> response = client.search(searchRequest, HotelDoc.class);
        System.out.println("response.toString() = " + response.toString());
        //最大分数
        System.out.println(response.maxScore());
        //分片数
        System.out.println(response.shards());
        //是否超时
        System.out.println(response.timedOut());
        //拿到匹配的数据
        HitsMetadata<HotelDoc> hitsMetadata = response.hits();
        //得到总数
        System.out.println(hitsMetadata.total());
        //拿到hits命中的数据
        List<Hit<HotelDoc>> hits = hitsMetadata.hits();
        for (Hit<HotelDoc> hit : hits) {
            //拿到_source中的数据
            System.out.println(hit.source());
            System.out.println(hit.index());
            System.out.println(hit.id());
            System.out.println(hit.score());
        }
    }

    @Test
    void testDeleteDocument() throws IOException {
        DeleteResponse deleteResponse = client.delete(i -> i.index("hotel").id("1"));
        Result result = deleteResponse.result();
        System.out.println("Deleted".equals(result.toString())?"删除成功":"没有找到");
    }

    @Test
    void testUpdateDocument() throws IOException {
        HotelDoc hotelDoc = new HotelDoc();
        hotelDoc.setPrice(952);
        hotelDoc.setStarName("四钻");
        // 1.准备请求参数
        UpdateRequest<String, HotelDoc> updateRequest = UpdateRequest.of(s -> s
                .index("hotel")
                .id("1")
                .doc(hotelDoc));
        UpdateResponse updateResponse = client.update(updateRequest, HotelDoc.class);
        System.out.println(updateResponse.version());
    }

    @Test
    void testBulkRequest() throws IOException {
        // 批量查询酒店数据
        List<Hotel> hotels = hotelService.list();
        // 创建 BulkOperation 列表
        ArrayList<BulkOperation> bulkOperations = new ArrayList<>();
        // 遍历酒店列表，为每个酒店添加 BulkOperation
        hotels.forEach(hotel -> {
            // 将 Hotel 转换为 HotelDoc
            HotelDoc hotelDoc = new HotelDoc(hotel);
            bulkOperations.add(BulkOperation.of(b -> b
                    .index(i -> i
                            .index("hotel")             // 索引名称
                            .id(String.valueOf(hotelDoc.getId()))  // 设置文档 ID
                            .document(hotelDoc)             // 设置文档内容
                    )
            ));
        });
// 构建 BulkRequest
        BulkRequest bulkRequest = BulkRequest.of(br -> br.operations(bulkOperations));
        // 发送 BulkRequest 请求
        BulkResponse bulkResponse = client.bulk(bulkRequest);
        // 打印批量操作的结果
        if (bulkResponse.errors()) {
            System.err.println("Bulk operation completed with errors.");
            // 遍历批量操作的结果，打印失败项的信息
            bulkResponse.items().forEach(item -> {
                if (item.error() != null) {
                    System.err.println("Error for document ID " + item.id() + ": " + item.error().reason());
                }
            });
        } else {
            System.out.println("Bulk operation successful.");
        }
    }
}
