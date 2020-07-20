// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

import org.apache.spark.sql.types._
val myDataSchema = StructType(
    Array(
        StructField("id_persona", DecimalType(26, 0), true), 
        StructField("anio", IntegerType, true), 
        StructField("mes", IntegerType, true), 
        StructField("provincia", IntegerType, true), 
        StructField("canton", IntegerType, true), 
        StructField("area", StringType, true), 
        StructField("genero", StringType, true), 
        StructField("edad", IntegerType, true), 
        StructField("estado_civil", StringType, true), 
        StructField("nivel_de_instruccion", StringType, true), 
        StructField("etnia", StringType, true), 
        StructField("ingreso_laboral", IntegerType, true), 
        StructField("condicion_actividad", StringType, true), 
        StructField("sectorizacion", StringType, true), 
        StructField("grupo_ocupacion", StringType, true), 
        StructField("rama_actividad", StringType, true), 
        StructField("factor_expansion", DoubleType, true)
    ));

// COMMAND ----------

val data = spark
  .read
  .schema(myDataSchema)
//  .option("inferSchema", true)
  .option("header", "true")
  .option("delimiter", "\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv");

// COMMAND ----------

data.printSchema

// COMMAND ----------

data.select("etnia").distinct().show

// COMMAND ----------

display(data.select("etnia").groupBy("etnia").count.orderBy("etnia"))

// COMMAND ----------

// MAGIC %md
// MAGIC • Cantidad de hombres y mujeres en las diferentes etnias

// COMMAND ----------

data.groupBy("etnia").pivot("genero").count.show()

// COMMAND ----------

display(data.groupBy("etnia").pivot("genero").count)

// COMMAND ----------

// MAGIC %md
// MAGIC • Cantidad de nulos en ingreso laboral dependiendo del genero en las distintas etnias

// COMMAND ----------

data.where($"ingreso_laboral".isNull).groupBy("etnia").pivot("genero").count.orderBy("etnia").show

// COMMAND ----------

// MAGIC %md
// MAGIC • Minimo de ingreso laboral dependiendo del genero en las distintas etnias

// COMMAND ----------

data.groupBy("etnia").pivot("genero").min("ingreso_laboral").orderBy("etnia").show

// COMMAND ----------

// MAGIC %md
// MAGIC • Maximo de ingreso laboral dependiendo del genero en las distintas etnias

// COMMAND ----------

data.groupBy("etnia").pivot("genero").max("ingreso_laboral").orderBy("etnia").show

// COMMAND ----------

// MAGIC %md
// MAGIC • El promedio del ingreso laboral dependiendo del genero en las distintas etnias

// COMMAND ----------

data.groupBy("etnia").pivot("genero").avg("ingreso_laboral").orderBy("etnia").show

// COMMAND ----------

// MAGIC %md
// MAGIC • Promedio de edad en las diferentes etnias

// COMMAND ----------

display(data.groupBy("etnia").pivot("genero").agg(round(avg("edad")).cast(IntegerType)).orderBy("etnia"))

// COMMAND ----------

data.select("estado_civil").distinct().show

// COMMAND ----------

// etnia con mayor numero de encuestados
val dataMestizos = data.where($"etnia" === "6 - Mestizo")

// COMMAND ----------

// MAGIC %md
// MAGIC • Cantidad en los diferentes estados civiles de la etnia mestizo

// COMMAND ----------

display(dataMestizos.groupBy("etnia").pivot("estado_civil").count.orderBy("etnia"))

// COMMAND ----------

data.select("condicion_actividad").distinct().show

// COMMAND ----------

// MAGIC %md
// MAGIC • Cantidad en las diferentes condiciones de actividad de la etnia mestizo

// COMMAND ----------

display(dataMestizos.groupBy("etnia").pivot("condicion_actividad").count.orderBy("etnia"))

// COMMAND ----------


