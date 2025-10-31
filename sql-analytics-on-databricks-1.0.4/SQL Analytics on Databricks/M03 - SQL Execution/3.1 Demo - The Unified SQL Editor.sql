-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3.1 Demo - O Editor SQL Unificado
-- MAGIC
-- MAGIC Nesta demonstração, vamos comparar escrever queries SQL em um notebook versus usar o Editor do Databricks SQL.
-- MAGIC
-- MAGIC
-- MAGIC ### Objetivos
-- MAGIC - Entenda como usar o Modo Foco para SQL dentro de um notebook.
-- MAGIC - Explore os recursos e capacidades do Editor SQL Unificado.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OBRIGATÓRIO - SELECIONE UM SHARED SQL WAREHOUSE
-- MAGIC
-- MAGIC Antes de executar células neste Notebook, selecione o **SHARED SQL WAREHOUSE** no laboratório. Siga estes passos:
-- MAGIC
-- MAGIC 1. Navegue até o canto superior direito deste Notebook e clique na lista drop-down para selecionar computação (pode dizer **Conectar**). Preencha um dos itens a seguir abaixo:
-- MAGIC
-- MAGIC    a. Em **recursos recentes**, verifique se você tem um **SQL shared_warehouse**. Se tiver, selecione-o.
-- MAGIC
-- MAGIC    b. Se você não tiver um **shared_warehouse** em **recursos recentes**, conclua o seguinte:
-- MAGIC
-- MAGIC     - Na mesma lista drop-down, selecione **Mais**.
-- MAGIC
-- MAGIC     - Em seguida, selecione o botão **SQL Warehouse**.
-- MAGIC
-- MAGIC     - Na lista drop-down, verifique se **shared_warehouse** está selecionado.
-- MAGIC
-- MAGIC     - Em seguida, na parte inferior do pop-up, selecione **Iniciar e anexar**.
-- MAGIC
-- MAGIC <br></br>
-- MAGIC    <img src="../Includes/images/sql_warehouse.png" alt="SQL Warehouse" width="600">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Configuração da sala de aula
-- MAGIC
-- MAGIC Execute a célula a seguir para configurar o ambiente de trabalho para este notebook.
-- MAGIC
-- MAGIC **NOTA:** O objeto `DA` é usado apenas em cursos da Databricks Academy e não está disponível fora desses cursos. Ele fará referência dinâmica à informação necessária para executar o curso no ambiente de laboratório.
-- MAGIC
-- MAGIC ### Informações importantes do LABORATÓRIO
-- MAGIC
-- MAGIC Lembre-se de que a configuração do laboratório é criada com o [0 - OBRIGATÓRIO - Configuração do Curso]($../0 - REQUIRED - Course Setup and Data Discovery). Se você encerrar a sessão de laboratório ou se a sessão atingir o limite de tempo, seu ambiente será redefinido e você precisará executar novamente Configuração do curso do  Notebook.

-- COMMAND ----------

-- MAGIC %run ../Includes/3.1-Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a célula a seguir para exibir o catálogo e o esquema default. Confirme se seu catálogo default é **dbacademy**, e seu esquema é seu esquema **labuser**.

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Use a célula do notebook no modo de foco SQL
-- MAGIC O modo de foco de célula expande a célula e funciona para Python, SQL, markdown e muito mais. 
-- MAGIC
-- MAGIC - Ícone de **abrir modo de foco**: ![open_focus_mode.png](../Includes/images/focus_mode.png) 
-- MAGIC <br></br>
-- MAGIC - Ícone de **sair do modo de foco**: ![open_focus_mode.png](../Includes/images/focus_mode_exit.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Na célula abaixo, passe o rato sobre a célula e selecione o ícone **abrir modo de foco** ![open_focus_mode.png](../Includes/images/focus_mode.png). Isso abre a célula no modo de foco e fornece alguns recursos adicionais.

-- COMMAND ----------

-- Explorando o Modo de Foco

-- 1. Explore rapidamente o Modo de foco selecionando o ícone do modo de foco.

-- 2. Execute as queries selecionando o ícone "Executar célula" (mais) abaixo.

-- 3. Visualize vários resultados na saída e crie visualizações conforme necessário.

-- 4. Saia do Modo de Foco seleccionando o ícone no canto superior direito da célula.

SELECT *
FROM aus_customers;


SELECT *
FROM aus_opportunities;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Editor SQL unificado
-- MAGIC Ao longo deste curso, usamos um notebook SQL para executar o DBSQL. O uso do notebook nos permite incorporar texto markdown durante o treinamento.
-- MAGIC
-- MAGIC No entanto, como desenvolvedor, talvez você prefira usar um editor SQL nativo, especialmente se tiver experiência com um em outro ambiente de desenvolvimento SQL. No Databricks, você pode usar o Editor SQL Unificado para uma experiência mais focada no desenvolvedor com recursos específicos.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Copiar as instruções do Editor SQL Unificado

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Na navegação à esquerda, clique com o botão direito do rato no arquivo **3.1b - Unified SQL Editor Directions.txt** e selecione **Abrir em nova tab**. Em seguida, copie o conteúdo do arquivo para a área de transferência.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. Abrindo e usando o Editor SQL unificado
-- MAGIC
-- MAGIC 1. No painel de navegação principal à esquerda, clique com o botão direito do rato em **Editor SQL** e selecione **Abrir em nova tab**. Isso o levará diretamente ao editor.
-- MAGIC
-- MAGIC 2. Cole as instruções do passo **C1** que você copiou no editor de query. Em seguida, siga essas instruções.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
