package com.data.integration.core

import java.io.{File, FileInputStream, FilenameFilter}
import java.util.Properties

/**
  * 提供基本的获取配置方法
  * 不同的类型的程序所依赖的配置基于此类拓展
  */
trait Configuration extends Serializable {

  private val conf = new Properties()

  // TODO 改成固定的方式
  //上线
  val confPath = "./"
  //线下测试
  //private val confPath = "your current project path"

  private val files: Array[File] = new File(confPath).listFiles(new FilenameFilter {
    override def accept(dir: File, name: String): Boolean = name.endsWith(".properties")
  })

  files.foreach(file => {
    conf.load(new FileInputStream(file))
  })

  /**
    * 通过键获取字符串值
    * @param key key
    * @return
    */
  def getString(key: String): String = {
    conf.getProperty(key).trim
  }

  /**
    * 通过键获取数值
    * @param key key
    * @return
    */
  def getInt(key: String): Int = {
    conf.getProperty(key).trim.toInt
  }

  def getTableWhiteList: Array[String]

  def getTableIgnoreDeleteList: Array[String]

}
