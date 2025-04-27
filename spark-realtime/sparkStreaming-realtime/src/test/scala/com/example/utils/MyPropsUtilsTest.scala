package com.example.utils

import org.junit.jupiter.api.Test

class MyPropsUtilsTest {
  @Test
  def apply(): Unit = {
    println(MyPropsUtils("kafka.bootstrap-servers"))
  }
}
