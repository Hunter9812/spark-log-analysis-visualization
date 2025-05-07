package com.example.utils;

import com.example.entity.{DauInfo, PageLog}
import org.junit.jupiter.api.Test;


class MyBeanUtilsTest {

    @Test
    def copyProperties(): Unit = {
        val pageLog: PageLog =
            PageLog("mid1001" , "uid101" , "prov101" , null ,null ,null ,null ,null ,null ,null ,null ,null ,null ,0L ,null ,123456)

        val dauInfo: DauInfo = new DauInfo()
        println("拷贝前: " + dauInfo)

        MyBeanUtils.copyProperties(pageLog,dauInfo)

        println("拷贝后: " + dauInfo)
    }
}