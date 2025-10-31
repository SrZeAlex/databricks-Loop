-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Laboratório Genie Spaces
-- MAGIC
-- MAGIC No notebook *05 Criando Genie Spaces*, exploramos alguns recursos dos Genie spaces no Databricks. Vamos a usar as habilidades que aprendemos para obter respostas de percepções para perguntas sobre nossos dados.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pré-requisitos
-- MAGIC
-- MAGIC Neste notebook, criaremos um Genie space para interagir com o dataset TPC-DI. Para acompanhar, você deve ter percorrido o processo de ingestão TPC-DI, conforme descrito no notebook *02.1 Ingestão de Dados*.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criando um Genie Space
-- MAGIC
-- MAGIC Crie um novo Genie Space que use seu SQL warehouse e a tabela *dimcompany* do esquema *tpcdi_dbsql_10* do seu catálogo pessoal.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![Select table](images/genie/lab/newgeniespace.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Interagindo com os dados
-- MAGIC
-- MAGIC Elaborar um prompt para determinar quantas companhias aéreas foram fundadas na primeira metade do século XX?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Um prompt típico como o seguinte pode ser usado para obter esse resultado:<br>
-- MAGIC *Quantas companhias aéreas foram fundadas entre os anos de 1900 e 1950, inclusive?*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Solicite que o sistema visualize a distribuição das empresas por tipo de indústria.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Um prompt típico como o seguinte pode ser usado para obter esse resultado:<br>
-- MAGIC *Mostre-me a distribuição das empresas por tipo de indústria.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Você recebeu o seguinte código SQL:
-- MAGIC ```
-- MAGIC SELECT
-- MAGIC   `country`,
-- MAGIC   COUNT(`name`) AS `count`
-- MAGIC FROM
-- MAGIC   `dimcompany`
-- MAGIC GROUP BY
-- MAGIC   `country`
-- MAGIC ```
-- MAGIC Crie um prompt que alcance esse resultado.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Esta é uma contagem simples de empresas, agrupadas por país. Um prompt típico como o seguinte pode ser usado para obter esse resultado:<br>
-- MAGIC *Mostre-me a distribuição das empresas por país.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
-- MAGIC
