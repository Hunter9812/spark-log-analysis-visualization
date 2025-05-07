package com.example.utils

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

object MyBeanUtils {

  def copyProperties(srcObj: AnyRef, destObj: AnyRef): Unit = {
    require(srcObj != null && destObj != null, "Source and Target must not be null")

    val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields

    for (srcField <- srcFields) {
      Breaks.breakable {
        val getMethodName: String = srcField.getName
        val setMethodName: String = srcField.getName + "_$eq"

        try {
          val getMethod: Method = srcObj.getClass.getDeclaredMethod(getMethodName)
          val setMethod: Method = destObj.getClass.getDeclaredMethod(setMethodName, srcField.getType)

          getMethod.setAccessible(true)
          setMethod.setAccessible(true)

          val destField: Field = destObj.getClass.getDeclaredField(srcField.getName)
          destField.setAccessible(true)

          if ((destField.getModifiers & Modifier.FINAL) != 0) {
            // 是final字段，跳过
            Breaks.break()
          }

          val value: AnyRef = getMethod.invoke(srcObj)
          setMethod.invoke(destObj, value)

        } catch {
          case _: NoSuchMethodException =>
            // 目标对象没有对应方法，跳过
            Breaks.break()
          case _: NoSuchFieldException =>
            // 目标对象没有对应字段，跳过
            Breaks.break()
          case ex: Exception =>
            // 其他异常打印警告，但不中断整体拷贝
            println(s"Failed to copy field '${srcField.getName}': ${ex.getMessage}")
            Breaks.break()
        }
      }
    }
  }
}
