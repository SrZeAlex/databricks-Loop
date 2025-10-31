# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estratégias de Modelagem de Dados
# MAGIC
# MAGIC Este curso oferece um mergulho profundo no design de modelos de dados no ambiente Databricks Lakehouse e na compreensão do ciclo de vida dos produtos de dados. Os participantes aprenderão a alinhar os requisitos de negócios com a organização de dados e o design de modelos, aproveitando o Delta Lake e o Unity Catalog para definir arquiteturas de dados e técnicas de integração e compartilhamento de dados.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pré-requisitos
# MAGIC
# MAGIC O conteúdo foi desenvolvido para os participantes com as seguintes competências/conhecimentos/habilidades:
# MAGIC - Conhecimento básico de conceitos de data warehousing, incluindo esquemas, processos ETL e fluxos de trabalho de business intelligence.
# MAGIC - Familiaridade com SQL, incluindo criação de tabelas, joins, restrições e manipulação de dados usando queries.
# MAGIC - Experiência básica com Databricks ou plataformas de dados similares baseadas em nuvem.
# MAGIC - Conhecimento fundamental da arquitetura Lakehouse e operações Delta Lake.
# MAGIC - Exposição a Python e PySpark para processamento de dados e tarefas de transformação (recomendado, mas não obrigatório).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Agenda do Curso
# MAGIC
# MAGIC Os módulos a seguir fazem parte do curso **Estratégias de Modelagem de Dados** da Databricks Academy.
# MAGIC
# MAGIC | Nome do módulo | Conteúdo                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
# MAGIC | ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
# MAGIC | Modelagem de dados do data warehouse | **Palestra:** Recap de Lakehouse Architecture <br> **Palestra:** Visão geral da modelagem DWH <br> **Palestra:** Fábrica de Informações Corporativas da Inmon <br/> [Demonstração: 01 - Modelagem de Relacionamento de Entidades e Trabalhando com Restrições]($./1 - Entity Relationship Modeling and Working with Constraints) <br> **Palestra:** Modelagem Dimensional de Kimball <br/> [Demo: 02 - Modelagem Dimensional e ETL]($./2 - Dimensional Modeling and ETL) <br> **Palestra:** Data Vault 2.0 <br/> [Demonstração: 03 - Data Vault 2.0]($./3 - Data Vault 2.0) |
# MAGIC | Casos de uso de arquitetura de dados modernos | **Palestra:** Estudo de Caso Moderno: Repositório de recursos <br/> [Demonstração: 04 - Caso de uso moderno: Repositório de recursos no Databricks]($./4 - Modern Use Case: Feature Store on Databricks) <br/> [Demonstração: 05 - Desmontagem de Lab]($./5 - Lab Teardown) <br> **Palestra:** Combinando Abordagens |
# MAGIC | Produtos de dados | **Palestra:** Definição de produtos de dados |
# MAGIC | Laboratório abrangente | [Laboratório: 01 - Modelagem de Data Warehousing com ERM e Modelagem Dimensional no Databricks]($./6L - Data Warehousing Modeling with ERM and Dimensional Modeling in Databricks)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Requisitos
# MAGIC
# MAGIC Por favor, revise os seguintes requisitos antes de iniciar a lição:
# MAGIC
# MAGIC * Para executar notebooks de demonstração e laboratório, você precisa usar um dos seguintes tempos de execução do Databricks: **`15.4.x-scala2.12`**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
