# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Warehousing com Databricks
# MAGIC
# MAGIC Este curso foi desenvolvido para profissionais de dados que desejam explorar os recursos de Data Warehousing do Databricks. Supondo que não haja conhecimento prévio do Databricks, ele fornece uma introdução ao uso do Databricks como uma solução moderna de Data Warehousing baseada em nuvem. Os alunos explorarão como usar a Plataforma de Databricks Data Intelligence Platform, transformar, governar e analisar dados de forma eficiente usando o dataset TCP-DI padrão do setor como referência. Os alunos também explorarão o Genie, um recurso inovador do Databricks que simplifica a exploração de dados por meio de queries em linguagem natural. Ao final deste curso, os participantes serão equipados com as habilidades fundamentais para implementar e otimizar um data warehouse usando a Databricks.
# MAGIC
# MAGIC ###Pré-requisitos
# MAGIC
# MAGIC O conteúdo foi desenvolvido para participantes com estas competências/conhecimentos/capacidades:
# MAGIC
# MAGIC   - Noções básicas de SQL e conceitos de query de dados
# MAGIC   - Recomenda-se o conhecimento geral dos conceitos de data warehouse, incluindo tabelas, esquemas e processos ETL/ELT
# MAGIC   - Alguma experiência com ferramentas de BI e/ou visualização de dados é útil, mas não obrigatória.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda do Curso
# MAGIC Os módulos a seguir fazem parte do curso **Data Warehousing com Databricks** da Databricks Academy.
# MAGIC | # | Nome do Módulo |
# MAGIC | --- | --- |
# MAGIC | 1 | [01 - Preparando TPC-DI]($./01 - Preparing TPC-DI) |
# MAGIC | 2 | [02.1 - Ingestão de Dados]($./02.1 - Ingesting Data) |
# MAGIC |   | [02.2 - Explorando Dados]($./02.2 - Exploring Data) |
# MAGIC | 3 | [03 - Explorando a capacidade de fluxo de trabalho]($./03 - Exploring Workflow Capabilities) |
# MAGIC | 5 | [05 - Criando e Gerenciando um Painel]($./05 - Creating and Managing a Dashboard) |
# MAGIC |   | [05L - Criando um Painel de Laboratório]($./05L - Creating a Dashboard Lab) |
# MAGIC | 6 | [06 - Criando Genie Spaces]($./06 - Creating Genie Spaces) |
# MAGIC |   | [06L - Lab de Genie Spaces]($./06L - Genie Spaces Lab) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requisitos
# MAGIC
# MAGIC Por favor, revise os seguintes requisitos antes de iniciar a lição:
# MAGIC * Para executar notebooks de demonstração e laboratório, você precisa usar o seguinte tempo de execução do Databricks: **15.4.x-scala2.12**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC
