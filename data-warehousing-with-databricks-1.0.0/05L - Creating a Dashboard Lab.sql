-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Laboratório sobre Criação de Painel
-- MAGIC
-- MAGIC No notebook *04 Criando e gerenciando um painel*, exploramos alguns recursos básicos relacionados à criação de um painel. Vamos colocar essas habilidades que aprendemos a usar para criar um painel personalizado que rastreia as estatísticas da empresa.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pré-requisitos
-- MAGIC
-- MAGIC Neste laboratório, construiremos painéis de IA/BI baseados no dataset TPC-DI. Para acompanhar, você deve ter percorrido o processo de ingestão TPC-DI, conforme descrito no notebook *02.1 Ingestão de Dados*.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criando um painel com um dataset
-- MAGIC
-- MAGIC 1. Crie um novo painel.
-- MAGIC 1. Associe a tabela *dimcompany*, a partir do esquema *tpcdi_dbsql_10* do seu catálogo pessoal.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Adicionando visualizações ao painel

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visualizando a distribuição geográfica das empresas
-- MAGIC
-- MAGIC No canto superior esquerdo da tela, adicione um gráfico de pizza para visualizar o número de empresas por país.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visualizando a longevidade da empresa
-- MAGIC
-- MAGIC À direita da visualização anterior, adicione um gráfico de barras para visualizar o número de empresas fundadas a cada ano.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Filtrando dados

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Adicionando um filtro
-- MAGIC
-- MAGIC Abaixo do gráfico de pizza, adicione um widget que permita ao usuário filtrar dados com base em um setor específico.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Aplicando um filtro
-- MAGIC
-- MAGIC Use o filtro que você criou para determinar quantas empresas de água foram fundadas em 1961.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Quantas companhias aéreas canadenses existem?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
-- MAGIC
