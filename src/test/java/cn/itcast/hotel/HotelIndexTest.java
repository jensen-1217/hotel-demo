package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
public class HotelIndexTest {
    private RestClient client;

    @BeforeEach
    void setUp() {
        this.client = (RestClient.builder(
               new HttpHost("http://192.168.93.132:9200")
        ).build());
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }
}