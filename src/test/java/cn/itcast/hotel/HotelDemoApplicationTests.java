package cn.itcast.hotel;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class HotelDemoApplicationTests {

    @Autowired
    private HotelMapper hotelMapper;
    @Test
    void contextLoads() {
        Hotel hotel = hotelMapper.selectById(45845L);
        System.out.println("hotel = " + hotel);
    }

}
