-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4.1 Demonstração - Percepções de Query
-- MAGIC
-- MAGIC Nesta demonstração, você explorará o desempenho da query, exibirá o histórico de query do SQL warehouse e aprenderá outras técnicas para monitorar o SQL warehouse.
-- MAGIC
-- MAGIC ### Objetivos
-- MAGIC - Visualizar o desempenho de uma query em DBSQL para identificar áreas para otimização.
-- MAGIC
-- MAGIC - Explorar o perfil de uma query em DBSQL em alto nível para visualizar detalhes de execução.
-- MAGIC
-- MAGIC - Explore o histórico de query do seu SQL warehouse para acompanhar o uso e o desempenho das queries.
-- MAGIC
-- MAGIC - view técnicas para monitorar seu warehouse SQL usando o Databricks monitoring tool e tabelas do sistema para garantir o desempenho ideal e o uso de recursos.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OBRIGATÓRIO - SELECIONE UM SHARED SQL WAREHOUSE
-- MAGIC
-- MAGIC Antes de executar células neste Notebook, selecione o **SHARED SQL WAREHOUSE** no laboratório. Siga estes passos:
-- MAGIC
-- MAGIC 1. Navegue até o canto superior direito deste Notebook e clique na lista dropdown para selecionar computação (pode dizer **Conectar**). Preencha um dos itens a seguir abaixo:
-- MAGIC
-- MAGIC    a. Em **recursos recentes**, verifique se você tem um **SQL shared_warehouse**. Se tiver, selecione-o.
-- MAGIC
-- MAGIC    b. Se você não tiver um **shared_warehouse** em **recursos recentes**, conclua o seguinte:
-- MAGIC
-- MAGIC     - Na mesma lista dropdown, selecione **Mais**.
-- MAGIC
-- MAGIC     - Em seguida, selecione o botão **SQL Warehouse**.
-- MAGIC
-- MAGIC     - Na lista dropdown, verifique se **shared_warehouse** está selecionado.
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
-- MAGIC Lembre-se de que a configuração do laboratório é criada com o notebook [0 - OBRIGATÓRIO - Configuração do Curso]($../0 - REQUIRED - Course Setup and Data Discovery). Se você encerrar a sessão de laboratório ou se a sessão atingir o limite de tempo, seu ambiente será redefinido e você precisará executar novamente o Notebook Configuração do curso.

-- COMMAND ----------

-- MAGIC %run ../Includes/4.1-Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Veja o desempenho da sua query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. As queries abaixo visualizam dados nas tabelas **samples.tpch.customer** e **samples.tpch.orders**. Execute a célula abaixo e veja os resultados. Observe o seguinte:
-- MAGIC
-- MAGIC    - Por default, a última tabela, **samples.tpch.orders**, é mostrada na saída. Esta tabela contém informações sobre pedidos, incluindo a coluna **o_custkey**, que pode ser usada para juntar-se à tabela **clientes**.
-- MAGIC    
-- MAGIC    - Acima da saída, expanda **Mostrar 1 resultado adicional**. Isso exibirá a tabela **clientes**. Observe que a tabela **clientes** contém a coluna **c_custkey**.

-- COMMAND ----------

SELECT *
FROM samples.tpch.customer
LIMIT 5;

SELECT *
FROM samples.tpch.orders
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Na célula acima, observe que há uma lista dropdown chamada **Ver desempenho** logo abaixo da última instrução `LIMIT`. Preencha o seguinte:
-- MAGIC
-- MAGIC    a. Expandir **Ver desempenho**. Observe que ambas as queries que executamos estão disponíveis.
-- MAGIC
-- MAGIC    b. Selecione uma das queries. Um pop-up aparecerá ao lado com informações de query. Aqui, você pode examinar o desempenho de sua query.
-- MAGIC
-- MAGIC    c. Na parte inferior do pop-up, selecione **Ver perfil de query**. Isso exibirá todo o perfil da query para você examinar.
-- MAGIC
-- MAGIC    d. Feche o perfil da query selecionando o **X** no canto superior direito do perfil.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Vamos executar uma query mais complicada que inclui um **INNER JOIN** e um **GROUP BY**, e examinar o perfil da query em um nível alto. Você pode usar o perfil de query para visualizar os detalhes de uma execução de query. O perfil de query ajuda a solucionar gargalos de desempenho durante a execução da query.
-- MAGIC
-- MAGIC       **REQUISITOS:** Para visualizar um perfil de query, você deve ser o proprietário da query ou deve ter pelo menos a permissão CAN MONITOR no SQL warehouse que executou a query.
-- MAGIC
-- MAGIC    Preencha o seguinte:
-- MAGIC
-- MAGIC    A. Execute a célula abaixo.
-- MAGIC
-- MAGIC    b. Expandir **Ver desempenho**.
-- MAGIC
-- MAGIC    c. Selecione a query para ver o desempenho. Observe que essa query foi executada em cerca de ~2 segundos.
-- MAGIC
-- MAGIC    d. Selecione **Ver perfil de query**. Aqui, você pode examinar cada estágio do perfil de query para determinar o desempenho da query e identificar eventuais gargalos ou problemas. Não vamos nos aprofundar aqui, mas é bom entender o perfil à medida que você ganha mais experiência.
-- MAGIC
-- MAGIC    ![Query Plan](../Includes/images/query_plan_join_newUI.png)

-- COMMAND ----------

SELECT 
  c.c_mktsegment,
  o.o_orderstatus,
  count(*) AS total_count
FROM samples.tpch.customer c
  INNER JOIN samples.tpch.orders o ON c.c_custkey = o.o_custkey
GROUP BY
  c.c_mktsegment,
  o.o_orderstatus
ORDER BY
  c.c_mktsegment,
  o.o_orderstatus;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Recursos adicionais
-- MAGIC
-- MAGIC Esta foi uma rápida introdução ao perfil de query no Databricks. Para obter informações mais detalhadas, veja os seguintes recursos:
-- MAGIC
-- MAGIC - [Perfil de Query](https://docs.databricks.com/aws/en/sql/user/queries/query-profile)
-- MAGIC
-- MAGIC - [Conheça suas queries com o novo perfil de query do Databricks SQL!](https://www.databricks.com/blog/2022/02/23/get-to-know-your-queries-with-the-new-databricks-sql-query-profile.html)
-- MAGIC
-- MAGIC - [Cache de query](https://docs.databricks.com/aws/en/sql/user/queries/query-caching)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. View Histórico de query do warehouse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Você também pode visualizar o Histórico de query do seu data warehouse. Para visualizar todo o histórico de query do data warehouse, conclua o seguinte:
-- MAGIC
-- MAGIC    a. No painel de navegação à esquerda, clique com o botão direito do rato em **Computação** e selecione **Abrir em Nova tab**. Isso abrirá os recursos de computação disponíveis em seu ambiente.
-- MAGIC
-- MAGIC    b. Nas tabs de navegação superiores, selecione **SQL Warehouses** para visualizar seus warehouses disponíveis.
-- MAGIC
-- MAGIC    c. Você deve ver apenas um warehouse, **shared_warehouse** (sua organização pode ter vários warehouses aqui). Selecione o warehouse.
-- MAGIC
-- MAGIC    d. Você verá uma variedade de tabs aqui com base no seu acesso. Suas permissões neste ambiente de laboratório limitam o que você pode visualizar.
-- MAGIC
-- MAGIC    e. No canto superior direito da página, você deve ver as três reticências. Selecione-os e escolha **Histórico de Query**.
-- MAGIC
-- MAGIC    f. Percorra a página. Como este é um warehouse compartilhado, você verá uma variedade de queries, muitas das quais você não executou.
-- MAGIC
-- MAGIC    g. Feche a página do histórico de query.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Dependendo do seu acesso, você também poderá **Monitorar um SQL warehouse**. Neste laboratório, você não tem esse recurso, mas, se disponível, ele forneceria uma visualização de várias estatísticas para seu warehouse, permitindo que você monitore seu desempenho.
-- MAGIC
-- MAGIC    Vamos ver a página de documentação [Monitorar a SQL warehouse](https://docs.databricks.com/aws/en/compute/sql-warehouse/monitor)  para obter mais informações.
-- MAGIC
-- MAGIC
-- MAGIC ![Monitorando o SQL Warehouse](../Includes/images/monitoring_sql_warehouse.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Tabelas do sistema Databricks (visualização pública a partir de março de 2025)
-- MAGIC Você também pode usar tabelas do sistema no Databricks para monitorar uma variedade de informações de conta.
-- MAGIC
-- MAGIC As tabelas do sistema são um armazenamento analítico hospedado no Databricks dos dados operacionais da sua conta encontrados no catálogo do sistema. As tabelas do sistema podem ser usadas para observabilidade histórica em sua conta. Abaixo estão alguns links adicionais para monitorar seu SQL Warehouse e queries com tabelas do sistema. 
-- MAGIC
-- MAGIC - [Monitorar a atividade da conta com tabelas do sistema](https://docs.databricks.com/gcp/en/admin/system-tables/) - Lista de todas as tabelas do sistema
-- MAGIC
-- MAGIC   - [referência da tabela do sistema de histórico de queries](https://docs.databricks.com/aws/en/admin/system-tables/query-history) - Pré-visualização pública a partir de março de 2025
-- MAGIC
-- MAGIC   - [Referência da tabela do sistema de warehouse](https://docs.databricks.com/gcp/en/admin/system-tables/warehouses) - Pré-visualização pública a partir de março de 2025 
-- MAGIC
-- MAGIC   - [Referência da tabela do sistema de eventos do Warehouse](https://docs.databricks.com/gcp/en/admin/system-tables/warehouse-events) - Prévia pública a partir de março de 2025

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
