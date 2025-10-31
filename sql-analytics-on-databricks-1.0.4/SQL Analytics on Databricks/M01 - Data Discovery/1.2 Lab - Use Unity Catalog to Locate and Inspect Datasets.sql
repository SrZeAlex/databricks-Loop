-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.2 Laboratório - Usar o Unity Catalog para localizar e inspecionar datasets
-- MAGIC
-- MAGIC Neste laboratório você explorará seu ambiente.

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
-- MAGIC    b. Se você não tiver um **SQL shared_warehouse** em **recursos recentes**, conclua o seguinte:
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
-- MAGIC Lembre-se de que a configuração do laboratório é criada com o notebook [0 - OBRIGATÓRIO - Configuração do Curso]($../0 - REQUIRED - Course Setup and Data Discovery). Se você encerrar a sessão de laboratório ou se a sessão atingir o tempo limite, seu ambiente será redefinido e você precisará executar novamente o Notebook Configuração do curso.

-- COMMAND ----------

-- MAGIC %run ../Includes/1.2-Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Use o Catalog Explorer para responder às perguntas abaixo
-- MAGIC
-- MAGIC Use o Catalog Explorer para responder às perguntas abaixo da melhor maneira possível. Este conteúdo não é classificado, mas foi projetado para permitir que você pratique o que aprendeu neste módulo e explore a interface do usuário do Databricks Workspace.
-- MAGIC
-- MAGIC Para obter instruções detalhadas e as respostas, vá para o notebook **1.2 Solução de lab** deste laboratório.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Explore o Catálogo `samples`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Quem é o dono do catálogo **samples**?

-- COMMAND ----------

---- Use o Catalog Explorer para encontrar o proprietário

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Que permissões você tem no catálogo **samples**?

-- COMMAND ----------

---- Use o Catalog Explorer para encontrar as permissões

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Quantas tabelas estão no esquema **nyctaxi** (banco de dados) dentro do catálogo **samples**?

-- COMMAND ----------

---- Use o Catalog Explorer para visualizar as tabelas no esquema 'nyctaxi'.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Quantas colunas estão na tabela **samples.nyctaxi.trips**?

-- COMMAND ----------

---- Use o Catalog Explorer para visualizar as colunas na tabela 'trips'.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Explore o Catálogo `dbacademy_ca_sales`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Quais são os dois esquemas no catálogo **dbacademy_ca_sales**?

-- COMMAND ----------

---- Use a barra de navegação à esquerda para visualizar os esquemas.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Que tipo é o catálogo **dbacademy_ca_sales**?

-- COMMAND ----------

---- Use o Catalog Explorer para visualizar a seção 'About this catalog'.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Em **dbacademy_ca_sales.v01** navegue até a tabela **customers**. Que permissões você tem nesta tabela?

-- COMMAND ----------

---- Use o Catalog Explorer para visualizar as permissões na tabela 'customers'.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Explore o esquema do seu curso `dbacademy.labuser_name`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Quem criou a tabela **dbacademy.labuser_name.ca_orders**?

-- COMMAND ----------

---- Use o Catalog Explorer para visualizar os Detalhes da tabela 'ca_orders'.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Quais permissões você tem em sua tabela **ca_orders**?

-- COMMAND ----------

---- Use o Catalog Explorer para visualizar as permissões que você tem na tabela 'ca_orders'.


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Veja o histórico da tabela **ca_orders**. Qual é a versão desta tabela? Qual é a **Operação** mais recente?

-- COMMAND ----------

---- Use o Catalog Explorer para visualizar o Histórico.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Adicione uma *Descrição Sugerida de IA* à tabela **ca_orders**.

-- COMMAND ----------

---- Use o Catalog Explorer para adicionar a 'AI Suggested Description'.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Veja a história do **ca_orders**. Quantas versões essa tabela tem agora? Qual é a **Operação** mais recente?

-- COMMAND ----------

---- Use o Catalog Explorer para visualizar o Histórico

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
