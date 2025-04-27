package com.example.utils

import java.util.Properties
import java.io.InputStream

/**
 * 配置文件解析工具类
 */
object MyPropsUtils {
  private val env: String = {
    val baseProps = new Properties()
    val in: InputStream = getClass.getClassLoader.getResourceAsStream("application.properties")
    baseProps.load(in)
    baseProps.getProperty("current.env", "dev")
  }

  private val props: Properties = {
    val properties = new Properties()
    val fileName = s"application-$env.properties"
    val in: InputStream = getClass.getClassLoader.getResourceAsStream(fileName)
    if (in != null) {
      properties.load(in)
    } else {
      throw new RuntimeException(s"配置文件未找到: $fileName")
    }
    properties
  }

  def apply(key: String): String = {
    props.getProperty(key)
  }
}