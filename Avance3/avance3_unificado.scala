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

// grafico con la cantidad de escuestados en las diferentes etnias
display(data.select("etnia").groupBy("etnia").count.orderBy("etnia"))

// COMMAND ----------

// Cantidad de hombres y mujeres en las diferentes etnias
display(data.groupBy("etnia").pivot("genero").count)

// COMMAND ----------

// Minimo de ingreso laboral segun del genero en las distintas etnias
data.groupBy("etnia").pivot("genero").min("ingreso_laboral").orderBy("etnia").show

// COMMAND ----------

// Maximo de ingreso laboral segun del genero en las distintas etnias
data.groupBy("etnia").pivot("genero").max("ingreso_laboral").orderBy("etnia").show

// COMMAND ----------

// El promedio del ingreso laboral segun el genero en las distintas etnias
data.groupBy("etnia").pivot("genero").avg("ingreso_laboral").orderBy("etnia").show

// COMMAND ----------

// Cantidad de nulos en ingreso laboral segun del genero en las distintas etnias
data.where($"ingreso_laboral".isNull).groupBy("etnia").pivot("genero").count.orderBy("etnia").show

// COMMAND ----------

// Promedio de edad en las diferentes etnias
display(data.groupBy("etnia").pivot("genero").agg(round(avg("edad")).cast(IntegerType)).orderBy("etnia"))

// COMMAND ----------

data.select("condicion_actividad").distinct().show

// COMMAND ----------

// ingreso laboral promedio segun el genero y la condicion actividad en las diferentes etnias
display(data.groupBy("etnia", "condicion_actividad").pivot("genero").avg("ingreso_laboral").orderBy("etnia", "condicion_actividad"))

// COMMAND ----------

// etnia con mayor numero de encuestados
val dataMestizos = data.where($"etnia" === "6 - Mestizo")

// COMMAND ----------

data.select("estado_civil").distinct().show

// COMMAND ----------

// Cantidad de personas segun su estado civil con las diferentes condiciones de actividad en la etnia con mayor numero de encuestados
display(dataMestizos.groupBy("etnia", "condicion_actividad").pivot("estado_civil").count.orderBy("condicion_actividad"))

// COMMAND ----------

data.select("edad").distinct().show

// COMMAND ----------

val menorEdad = data.where($"edad" < 18)

// COMMAND ----------

val mayorEdad = data.where($"edad" >= 18)

// COMMAND ----------

// Promedio de ingreso laboral en los diferentes niveles de instruccion en las diferentes edades
data.groupBy("edad").pivot("nivel_de_instruccion").agg(round(avg("ingreso_laboral")).cast(IntegerType)).orderBy("edad").show

// COMMAND ----------

// Cual es el sueldo maximo que reciben las personas mayores de edad en sus diferentes rangos
mayorEdad.groupBy("edad").pivot("genero").max("ingreso_laboral").orderBy("edad").show

// COMMAND ----------

// Cual es el promedio del factor_expansion entre las personas menores de edad 
data.groupBy("edad", "etnia").pivot("genero").avg("factor_expansion").orderBy("edad").show

// COMMAND ----------

val rangoJoven = (mayorEdad.where($"edad" >= 23 && $"edad" <= 30))

// COMMAND ----------

// Cual es el numero de personas mayores de edad en que existen en los diferentes niveles de instruccion 
rangoJoven.groupBy("rama_actividad").pivot("edad").count.show(false)

// COMMAND ----------

 display(data.groupBy("edad").pivot("nivel_de_instruccion").count.orderBy("edad"))

// COMMAND ----------

display(data.select("rama_actividad").groupBy("rama_actividad").count)

// COMMAND ----------


