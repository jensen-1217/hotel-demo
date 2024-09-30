package cn.itcast.hotel;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.analysis.Analyzer;
import co.elastic.clients.elasticsearch._types.analysis.TokenFilterDefinition;
import co.elastic.clients.elasticsearch._types.analysis.Tokenizer;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.ObjectBuilder;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@SpringBootTest
public class TestIndex {

    @Autowired
    private RestClient restClient;
    @Autowired
    public ElasticsearchClient client;

    @Test
    public void testCreateHotelIndex() throws IOException {
        // 创建一个 Transport 和 JacksonJsonpMapper 序列化实例
//        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        // 创建 Elasticsearch 客户端
//        ElasticsearchClient client = new ElasticsearchClient(transport);
        // 创建索引
//        CreateIndexResponse createIndexResponse = client.indices().create(i ->
//                i.index("hotel").withJson(HotelConstants.MAPPING_TEMPLATE));
        CreateIndexResponse createIndexResponse = client.indices().create(i -> i
                .index("hotel")
                .mappings(s->s
                        .properties("id",n->n.keyword(nn->nn.index(true)))
                        .properties("city",n->n.keyword(nn->nn.index(true)))
                        .properties("starName",n->n.keyword(nn->nn.index(true)))
                        .properties("brand",n->n.keyword(nn->nn.index(true)))
                        .properties("location",n->n.geoPoint(nn->nn.index(true)))
                        .properties("name",n->n.text(nn->nn.analyzer("ik_max_word").copyTo("all")))
                        .properties("business",n->n.text(nn->nn.analyzer("ik_max_word").copyTo("all")))
                        .properties("address",n->n.text(nn->nn.analyzer("ik_max_word").copyTo("all")))
                        .properties("all",n->n.text(nn->nn.analyzer("ik_max_word")))
                        .properties("price",n->n.integer(nn->nn.index(true)))
                        .properties("score",n->n.integer(nn->nn.index(true)))
                )

        );
//        Map<String, Object> pyFilter = new HashMap<>();
//        pyFilter.put("type", "pinyin");
//        pyFilter.put("keep_full_pinyin", false);
//        pyFilter.put("keep_joined_full_pinyin", true);
//        pyFilter.put("keep_original", true);
//        pyFilter.put("limit_first_letter_length", 16);
//        pyFilter.put("remove_duplicated_term", true);
//        pyFilter.put("none_chinese_pinyin_tokenize", false);
//        CreateIndexResponse createIndexResponse = client.indices().create(i -> i
//                .index("hotel").settings(s->s
//                        .analysis(a->a
//                                .analyzer("text_analyzer",an->an
//                                        .custom(c->c.tokenizer("ik_max_word")
//                                                .filter("py")))
//                                .analyzer("completion_analyzer",an->an
//                                        .custom(c->c
//                                                .tokenizer("keyword")
//                                                .filter("py")))
//                                .filter("py",p->p.definition((TokenFilterDefinition) pyFilter))
//                        )
//                )
//                .mappings(s->s
//                        .properties("id",n->n.keyword(nn->nn.index(true)))
//                        .properties("city",n->n.keyword(nn->nn.index(true)))
//                        .properties("starName",n->n.keyword(nn->nn.index(true)))
//                        .properties("brand",n->n.keyword(nn->nn.index(true)))
//                        .properties("location",n->n.geoPoint(nn->nn.index(true)))
//                        .properties("name",n->n.text(nn->nn.analyzer("text_analyzer").searchAnalyzer("ik_smart").copyTo("all")))
//                        .properties("business",n->n.text(nn->nn.analyzer("ik_max_word").copyTo("all")))
//                        .properties("address",n->n.text(nn->nn.analyzer("ik_max_word").copyTo("all")))
//                        .properties("all",n->n.text(nn->nn.analyzer("text_analyzer").searchAnalyzer("ik_smart")))
//                        .properties("price",n->n.integer(nn->nn.index(true)))
//                        .properties("score",n->n.integer(nn->nn.index(true)))
//                        .properties("suggestion",n->n.completion(nn->nn.analyzer("completion_analyzer")))
//                )
//
//        );

        // 输出响应结果
        System.out.println("createIndexResponse.acknowledged() = " + createIndexResponse.acknowledged());
        System.out.println("createIndexResponse.shardsAcknowledged() = " + createIndexResponse.shardsAcknowledged());
        System.out.println("createIndexResponse.index() = " + createIndexResponse.index());
    }

    @Test
    public void testDeleteHotelIndex() throws IOException {
        DeleteIndexResponse deleteIndexResponse = client.indices().delete(i -> i.index("hotel"));
        System.out.println(deleteIndexResponse.acknowledged());
    }

    @Test
    public void testExistsHotelIndex() throws IOException {
        BooleanResponse exists = client.indices().exists(i -> i.index("hotel"));
        System.out.println(exists.value()?"索引库存在":"索引库不存在");
    }
}
