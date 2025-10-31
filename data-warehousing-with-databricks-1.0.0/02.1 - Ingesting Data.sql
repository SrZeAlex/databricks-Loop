-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Ingestão de dados

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pré-requisitos
-- MAGIC
-- MAGIC Neste notebook, exploraremos e executaremos um processo de ETL que ingerirá um dataset, de acordo com as convenções estabelecidas na especificação [TPC-DI](http://tpc.org/tpcdi/default5.asp). Para executar esta demonstração, você deve ter executado os preparativos descritos no notebook *01 Preparando TPC-DI*.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestão de dados
-- MAGIC
-- MAGIC Com os dados brutos de entrada prontos para ingestão, exploraremos e executaremos um Databricks workflow que criará e carregará uma coleção de tabelas que implementa as regras e os requisitos de negócios conforme estabelecido na especificação TPC.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Explorando o fluxo de trabalho
-- MAGIC
-- MAGIC Abra o fluxo de trabalho criado no notebook *01 Preparando TPC-DI* seguindo um destes procedimentos:
-- MAGIC 1. Siga o link fornecido na saída desse notebook.
-- MAGIC 1. Clique no item **Workflows** na barra lateral esquerda (abra-o em uma nova tab para que você possa continuar a seguir estas instruções).
-- MAGIC
-- MAGIC Abra a tab **Tasks** ou o fluxo de trabalho. Lá, você verá um DAG mostrando todas as tarefas para o fluxo de trabalho, sinta-se à vontade para clicar em tarefas individuais para explorar as configurações para a tarefa específica.
-- MAGIC
-- MAGIC 1. Vamos selecionar **ingest_ProspectIncremental**. Esta é uma tarefa do tipo **Notebook**. A Databricks oferece suporte a vários tipos de tarefas e esse tipo simplesmente executa um notebook.
-- MAGIC    * Chame sua atenção para o campo **Path**, que especifica a localização do notebook a ser executado. Se seguirmos isso, navegaremos até o notebook que foi chamado pela tarefa durante a execução.
-- MAGIC    * Vamos examinar as dependências da tarefa visualizando o campo **Depends**. Aqui, vemos que a tarefa depende de duas tarefas: *run_custmermgmt_YES_NO (false)* e *ingest_customermgmt*. Referindo-se ao campo **Run if dependencies**, vemos que a tarefa será executada se pelo menos uma das tarefas de dependência for bem-sucedida.
-- MAGIC 1. Agora vamos voltar nossa atenção para os parâmetros da tarefa. Se examinarmos o código do notebook, você notará que não há nomes codificados:
-- MAGIC    ```
-- MAGIC    INSERT INTO ${catalog}.${tgt_db}.${table}
-- MAGIC    SELECT * FROM ${catalog}.${wh_db}_${scale_factor}_stage.${view}
-- MAGIC    ```
-- MAGIC    Isso torna os notebooks muito mais portáteis, já que não há suposições internas sobre os nomes de catálogo, esquema ou tabela. Os Databricks Workflows facilitam a definição dos parâmetros no nível da tarefa ou do trabalho e fazem referência a esses parâmetros no código executado usando o padrão `${parameter_name}`. Como você pode ver, os parâmetros para as tarefas atuais são derivados a partir do parâmetro do job. Durante a execução, o parâmetro referenciado será substituído pelos valores definidos na tarefa de fluxo de trabalho.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Executando o fluxo de trabalho
-- MAGIC
-- MAGIC Clique no botão **Run now** no canto superior direito. Isso executará todas as tarefas no fluxo de trabalho usando o SQL warehouse predeterminado. Quando todo o trabalho for concluído, você poderá ver as execuções históricas na tab **Runs**, que lista todas as execuções anteriores, bem como o estado resultante. Isso permite que você monitore e acompanhe as execuções históricas para melhorias de desempenho ou diagnósticos de erros. 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
-- MAGIC
