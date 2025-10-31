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

-- Resposta à pergunta: Databricks, Inc

-- 1. Encontre o catálogo de 'samples' no painel de navegação esquerdo.

-- 2. Selecione as três reticências à direita do catálogo e selecione "Open in Catalog Explorer".

-- 3. À direita, em 'About this catalog', note que o proprietário deste catálogo é 'Databricks, Inc.'.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Que permissões você tem no catálogo **samples**?

-- COMMAND ----------

-- Resposta à pergunta: Nenhuma. As permissões não podem ser visualizadas ou editadas em dados de amostras.

-- 1. Encontre o catálogo 'samples' no painel de navegação esquerdo.

-- 2. Selecione as três reticências à direita do catálogo e selecione "Open in Catalog Explorer".

-- 3. Selecione o separador "Permissions" para visualizar as permissões.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Quantas tabelas estão no esquema **nyctaxi** (banco de dados) dentro do catálogo **samples**?

-- COMMAND ----------

-- Resposta à pergunta: 1 tabela chamada 'trips'

-- 1. Encontre o catálogo 'samples' no painel de navegação esquerdo.

-- 2. Selecione as três reticências à direita do catálogo e selecione "Open in Catalog Explorer".

-- 3. Selecione o esquema 'nyctaxi'.

-- 4. Deverá ver que o esquema 'nyctaxi' contém 1 tabela denominada 'trips'.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Quantas colunas estão na tabela **samples.nyctaxi.trips**?

-- COMMAND ----------

-- Resposta à pergunta: Existem 6 colunas na tabela "trips".

-- 1. Encontre o catálogo 'samples' no painel de navegação esquerdo.

-- 2. Selecione as três reticências à direita do catálogo e selecione "Open in Catalog Explorer".

-- 3. Selecione o esquema 'nyctaxi'.

-- 4. Selecione a tabela 'trips'.

-- 5. Consulte a visão geral da tabela. Aqui deve ver as colunas na tabela.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Explore o Catálogo `dbacademy_ca_sales`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Quais são os dois esquemas no catálogo **dbacademy_ca_sales**?

-- COMMAND ----------

-- Resposta à pergunta: 2 esquemas, 'information_schema' e 'v01'

-- 1. Encontre o catálogo 'dbacademy_ca_sales' no painel de navegação esquerdo.

-- 2. Expanda o catálogo.

-- 3. Visualize os dois esquemas dentro do catálogo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Que tipo é o catálogo **dbacademy_ca_sales**?

-- COMMAND ----------

-- Resposta à pergunta: O tipo é 'Delta Sharing' porque este catálogo foi criado utilizando o Databricks Marketplace quando o seu laboratório foi configurado.

-- 1. Encontre o catálogo 'dbacademy_ca_sales' no painel de navegação esquerdo.

-- 2. Selecione as três reticências à direita do catálogo e selecione "Open in Catalog Explorer".

-- 3. No separador "Overview", olhe para a extrema direita em "About this catalog". Note que o Tipo é 'Delta Sharing'.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Em **dbacademy_ca_sales.v01** navegue até a tabela **clientes**. Que permissões você tem nesta tabela?

-- COMMAND ----------

-- Pergunta/Resposta: 'SELECT'
 
-- 1. Encontre o catálogo 'dbacademy_ca_sales' no painel de navegação esquerdo.
 
-- 2. Selecione as três reticências à direita do catálogo e selecione 'Abrir no Catalog Explorer'.
 
-- 3. Selecione o esquema 'v01'.
 
-- 4. Selecione a tabela 'customers'.
 
-- 5. Selecione o tab 'Permissões'.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Explore o esquema do seu curso `dbacademy.labuser_name`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Quem criou a tabela **dbacademy.labuser_name.ca_orders**?

-- COMMAND ----------

-- Resposta à pergunta: A tabela foi criada pelo seu nome 'labuser' (exemplo, 'labuser1234_5678').

-- 1. Encontre o catálogo 'dbacademy' no painel de navegação esquerdo.

-- 2. Selecione as três reticências à direita do catálogo e selecione "Open in Catalog Explorer".

-- 3. Selecione o esquema 'labuser_name'.

-- 4. Selecione a tabela 'ca_orders'.

-- 5. Selecione o separador "Details".

-- 6. Localize a linha "Created by". Esta tabela foi criada pela sua conta através do script de configuração.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Quais permissões você tem em sua tabela **ca_orders**?

-- COMMAND ----------

-- Resposta à pergunta: 'ALL PRIVILEGES'

-- 1. Encontre o catálogo 'dbacademy' no painel de navegação esquerdo.

-- 2. Selecione as três reticências à direita do catálogo e selecione "Open in Catalog Explorer".

-- 3. Selecione o esquema 'labuser_name'.

-- 4. Selecione a tabela 'ca_orders'.

-- 5. Selecione o separador "Permissions".

-- 6. Note que tem 'ALL PRIVILEGES' nesta tabela, uma vez que é o proprietário.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Veja o histórico da tabela **ca_orders**. Qual é a versão desta tabela? Qual é a **Operação** mais recente?

-- COMMAND ----------

-- Resposta à pergunta: 'Version 0', 'CREATE TABLE AS SELECT'

-- 1. Encontre o catálogo 'dbacademy' no painel de navegação esquerdo.

-- 2. Selecione as três reticências à direita do catálogo e selecione "Open in Catalog Explorer".

-- 3. Selecione o esquema 'labuser_name'.

-- 4. Selecione a tabela 'ca_orders'.

-- 5. Selecione o separador "History".

-- 6. Note que esta tabela tem 1 versão, 'Version 0', e esta tabela foi criada utilizando a instrução 'CREATE TABLE AS SELECT'.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Adicione uma *Descrição Sugerida de IA* à tabela **ca_orders**.

-- COMMAND ----------

-- Resposta à pergunta: A sua tabela deve ter uma descrição.

-- 1. Encontre o catálogo 'dbacademy' no painel de navegação esquerdo.

-- 2. Selecione as três reticências à direita do catálogo e selecione "Open in Catalog Explorer".

-- 3. Selecione o esquema 'labuser_name'.

-- 4. Selecione a tabela 'ca_orders'.

-- 5. Selecione o separador "Overview".

-- 6. Note que está disponível uma 'AI Suggested Description' para a tabela. Selecione "Accept".

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Veja a história do **ca_orders**. Quantas versões essa tabela tem agora? Qual é a **Operação** mais recente?

-- COMMAND ----------

-- Resposta à pergunta: '2 Versions', 'version 0', 'version 1', 'SET TBLPROPERTIES'

-- 1. Encontre o catálogo 'dbacademy' no painel de navegação esquerdo.

-- 2. Selecione as três reticências à direita do catálogo e selecione "Open in Catalog Explorer".

-- 3. Selecione o esquema 'labuser_name'.

-- 4. Selecione a tabela 'ca_orders'.

-- 5. Selecione o separador "History".

-- 6. Note que esta tabela tem '2 versions', 'Version 0' e 'Version 1'. Uma nova versão desta tabela foi adicionada quando adicionou a 'AI generated description' desta tabela.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
