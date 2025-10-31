# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar pipelines de dados com o Delta Live Tables
# MAGIC Ao programar tarefas com os Databricks Jobs, os aplicativos podem ser executados automaticamente para manter as tabelas no lakehouse atualizadas. Usar Databricks SQL para programar atualizações em queries e painéis permite percepções rápidas usando os dados mais recentes. Neste curso, os alunos serão apresentados a orquestração de tarefas usando a interface do usuário dos Databricks Workflow Jobs. Opcionalmente, eles configurarão e irão programar painéis e alertas para refletir as atualizações dos pipelines de dados de produção.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pré-requisitos
# MAGIC Você deve atender aos seguintes pré-requisitos antes de iniciar este curso:
# MAGIC
# MAGIC - Familiaridade iniciante com conceitos básicos de nuvem (máquinas virtuais, armazenamento de objetos, gerenciamento de identidades)
# MAGIC - Capacidade de executar tarefas básicas de desenvolvimento de código (criar computação, executar código em notebooks, usar operações básicas de notebook, importar repos do git, etc.)
# MAGIC - Familiaridade intermediária com conceitos básicos de SQL (CREATE, SELECT, INSERT, UPDATE, DELETE, WHILE, GROUP BY, JOIN, etc.)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Agenda do Curso
# MAGIC
# MAGIC Os seguintes módulos fazem parte do curso **SQL Analytics on Databricks** da **Databricks Academy**.
# MAGIC
# MAGIC | Nome do Módulo &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Conteúdo &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|
# MAGIC | **M00: Getting Started** | [**0 - REQUIRED** - Course Setup and Data Discovery]($./0 - REQUIRED - Course Setup and Data Discovery) | 
# MAGIC | **[M01 - Data Discovery]($./M01 - Data Discovery)** | **Palestra -** Usando o Unity Catalog como uma ferramenta de descoberta de dados </br> **Palestra -** Compreendendo a propriedade de objetos de dados </br> [**1.2 Lab** - Use Unity Catalog to Locate and Inspect Datasets]($./M01 - Data Discovery/1.2 Lab - Use Unity Catalog to Locate and Inspect Datasets) | 
# MAGIC | **[M02 - Data Importing]($./M02 - Data Importing)** | **Palestra -** Ingestão de dados no Databricks </br> [**2.1 Demo** - Uploading Data to Databricks Using the UI]($./M02 - Data Importing/2.1 Demo - Uploading Data to Databricks Using the UI) </br> [**2.2 Demo** - Programmatic Exploration and Data Ingestion to Unity Catalog]($./M02 - Data Importing/2.2 Demo - Programmatic Exploration and Data Ingestion to Unity Catalog) </br> [**2.3 Lab** - Import Data into Databricks]($./M02 - Data Importing/2.3 Lab - Import Data into Databricks) | 
# MAGIC | **[M03 - SQL Execution]($./M03 - SQL Execution)** | **Palestra -** Databricks SQL e  SQL warehouse do Databricks </br> [**3.1 Demo** - The Unified SQL Editor]($./M03 - SQL Execution/3.1 Demo - The Unified SQL Editor) </br> [**3.1b** - Unified SQL Editor Directions]($./M03 - SQL Execution/3.1b - Unified SQL Editor Directions.txt) </br> [**3.2 Demo** - Manipulate and Transform Data with Databricks SQL]($./M03 - SQL Execution/3.2 Demo - Manipulate and Transform Data with Databricks SQL) </br> [**3.3 Demo** - Creating Views with Databricks SQL]($./M03 - SQL Execution/3.3 Demo - Creating Views with Databricks SQL) </br> [**3.4 Lab** - Manipulate and Analyze a Table]($./M03 - SQL Execution/3.4 Lab - Manipulate and Analyze a Table)| 
# MAGIC | **[M04 - Query Analysis]($./M04 - Query Analysis)** | **Palestra -** Databricks Photon e Otimização no Databricks </br> [**4.1 Demo** - Query Insights]($./M04 - Query Analysis/4.1 Demo - Query Insights) </br> **Palestra -** Melhores Práticas para Análises SQL</br>
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Requisitos
# MAGIC Certifique-se do seguinte antes de começar:
# MAGIC
# MAGIC - Use a versão do Databricks Runtime: **`SQL Warehouse`** para executar todos os notebooks de demonstração e laboratório.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
