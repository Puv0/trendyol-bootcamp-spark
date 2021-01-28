package com.trendyol.bootcamp.homework


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.hadoop.fs._

import scala.reflect.io.Directory
import java.io.File
import java.nio.file.Files
import scala.reflect.io.File
import scala.util.Try

object ProductMergerJob {

  def main(args: Array[String]): Unit = {

    /**
    * Find the latest version of each product in every run, and save it as snapshot.
    *
    * Product data stored under the data/homework folder.
    * Read data/homework/initial_data.json for the first run.
    * Read data/homework/cdc_data.json for the nex runs.
    *
    * Save results as json, parquet or etc.
    *
    * Note: You can use SQL, dataframe or dataset APIs, but type safe implementation is recommended.
    */

    /**
      * System.setProperty("hadoop.home.dir", "full path to the folder with winutils");
      *
      *
      *  Elimizdeki datayı ve yeni gelen datayı okuyup, bunları birleştirmemiz eğer
      *  yeni datalardan bazıları eski dataların değişmesini içeriyorsa, yeni dataları tercih edeceğimiz  bir batch job yazıldı.
      *  TypeSafe olabilmesi için dataya uygun bir caseClass tanımlandı ve datasetler bu type ile oluşturuldu.
      *  Job ilk çalıştığında eğer elimizde hiç data yoksa, initial_datadan okunması beklenmektedir. Bu sebeple, data/homework/output yolunun kontrolü yapılır, eğer oluşturulmamışsa
      *  initial_data okunur.
      *
      *  Eğer bu yol varsa, ikinci çalıştırma kısmında, önce eldeki data sonrasında yeni gelen data okunur. Yeni gelen datada eğer aynı id de birden fazla row olması demek datanın birden çok
      *  güncellenmesi demek olduğu için aynı id'ye sahip timestampi daha büyük olan kayıtları, window function yardımıyla row_number() verilir. Timestampi büyük olan ilk rank'de olacağı için
      *  diğerleri droplanır. Bu sayede birleştirmeye hazır iki datasetimiz hazırlanmış olur.
      *  Sonrasında, oldProduct'ı newProduct'la outer joinWith ile birleştirilir. Bu sayede
      *  dataya iki tupple gibi ulaşılabilir ve ikinci kısımın, yani newProduct'ın null olması demek, bu kayıtların değişmemesi anlamına gelir ve eski data aynen yazılır.
      *  Eğer oldProduct null ise, demek ki yeni bir kayıt eklenmiş demektir bu sebeple newProduct'taki kayıt eklenir. Eğer iki tarafta null değilse, newProduct yazılır.
      *  Bu şekilde, değişen ve yeni eklenen datalarımız data/homework/output yoluna yazılır.
      *
      *
      */
    System.setProperty("hadoop.home.dir","C:\\winutils")
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Product Merger Batch Job")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val productSchema = Encoders.product[ProductData].schema
    if(!scala.reflect.io.File("data/homework/output/").exists){
      // First Run
      val products = spark.read
          .schema(productSchema)
          .json("data/homework/initial_data.json")
          .as[ProductData]

      products
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .json("data/homework/output")

    }
    else {

      val oldProduct = spark.read
        .schema(productSchema)
        .json("data/homework/output/*")
        .as[ProductData]


      val newProduct:Dataset[ProductData] = spark.read
        .schema(productSchema)
        .json("data/homework/cdc_data.json")
        .withColumn("rank",  row_number().over(Window.partitionBy($"id").orderBy($"id",$"timestamp".desc)))
       .filter($"rank" ===1)
        .drop("rank")
        .as[ProductData]


      val joined = oldProduct.as("o").joinWith(newProduct.as("n"), oldProduct("id") === newProduct("id"), "outer")
        joined.show()

      val merger = joined
        .map {
          case (oldP, newP) =>
            (oldP, newP) match {
          case (_, _) if newP == null => ProductData(oldP.id, oldP.name, oldP.category, oldP.brand, oldP.color, oldP.price, oldP.timestamp)
          case (_, _) if oldP == null => ProductData(newP.id, newP.name, newP.category, newP.brand, newP.color, newP.price, newP.timestamp)
          case (_, _) if newP.timestamp > oldP.timestamp => ProductData(newP.id, newP.name, newP.category, newP.brand, newP.color, newP.price, newP.timestamp)
        }}
        .as[ProductData]
        .orderBy($"id",$"timestamp".desc)

      merger.show(false)
    /*
      merger
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .json("data/homework/output2")

      val file = scala.reflect.io.File("data/homework/output/")
      file.deleteRecursively()

      import java.nio.file.Paths
      val source = Paths.get("data/homework/output2/")
      val newdir = Paths.get("data/homework/output/")
      Files.move(source, newdir.resolve("../output2"))
   val okey = mv("output2","output")
      println(okey)

      Files.move(new File("data/homework/output2"), new File("<path to>"))
      */

    }


  }
  /*
  def mv(oldName: String, newName: String) =
    Try(
       File(oldName).renameTo(new File(newName)))
      .getOrElse(false)
    */
  case class ProductData(id: Long, name: String, category: String, brand: String ,color: String, price : Double, timestamp: Long)

}
