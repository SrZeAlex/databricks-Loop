-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Criando Genie Spaces
-- MAGIC
-- MAGIC Os Databricks Genie Spaces permitem que os usuários empresariais interajam com dados usando perguntas e respostas alimentadas por um Large Language Model (LLM). Os usuários finais podem fazer perguntas em linguagem natural e receber respostas em texto e visualizações.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pré-requisitos
-- MAGIC
-- MAGIC Neste notebook, criaremos um Genie space para interagir com o dataset TPC-DI. Para acompanhar, você deve ter percorrido o processo de ingestão TPC-DI, conforme descrito no notebook *02.1 Ingestão de Dados*.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criando um Genie Space
-- MAGIC
-- MAGIC 1. Acesse o Genie selecionando-o na barra lateral esquerda.<br>
-- MAGIC    ![Genie menu item](images/genie/sidebar.png)
-- MAGIC 1. Crie um novo espaço clicando em **New** no canto superior direito.
-- MAGIC 1. Especifique os dados aos quais conectar o Genie space:
-- MAGIC    * Selecione seu catálogo pessoal.
-- MAGIC    * Escolha o esquema *tpcdi_dbsql_10*.
-- MAGIC    * Selecione a tabela *dimcustomer*.<p>
-- MAGIC    * Clique em **Create**.<br>
-- MAGIC    ![Connecct data dialog](images/genie/configurespace.png)
-- MAGIC
-- MAGIC Seu Genie space agora está pronto para uso!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Interagindo com seus dados
-- MAGIC
-- MAGIC Com um Genie space criado, agora podemos começar a fazer perguntas sobre nossos dados em linguagem simples.
-- MAGIC
-- MAGIC 1. Vamos começar com um exemplo simples. Tente enviar um prompt como este: *Show me all the customer data.*</br>
-- MAGIC    ![Data tab](images/genie/firstprompt.png)
-- MAGIC 1. Uma resposta é gerada. Clique no botão **Show code** para expor o código SQL que foi gerado para satisfazer a solicitação. Isso nos mostra que os usuários podem fazer perguntas em inglês simples e, por meio do mecanismo de processamento com tecnologia de LLM do Databricks, isso é traduzido para SQL que fornecerá respostas ou que você poderá personalizar ainda mais. Esse recurso reduz drasticamente a barreira de geração de percepções rápidas para operadores de dados e usuários empresariais. <br>
-- MAGIC    ![Data tab](images/genie/showcode.png)
-- MAGIC 1. Vamos tentar um prompt diferente. Desta vez, envie um prompt como este: *Show me the distribution of customers based on gender.*</br>
-- MAGIC    ![Data tab](images/genie/secondprompt.png)<br>
-- MAGIC    Observe como desta vez uma visualização é gerada como parte da resposta.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Histórico
-- MAGIC
-- MAGIC Usando o histórico, você pode acessar todas as conversas anteriores no mesmo Genie space. 
-- MAGIC
-- MAGIC 1. Clique no botão **History** no canto superior direito.<br>
-- MAGIC    ![History button](images/genie/historybutton.png)<br>
-- MAGIC    Isso mostrará uma lista histórica de conversas, para que você possa monitorar a atividade dentro do seu espaço, entender erros, feedback e resultados que o espaço está retornando para seus usuários.
-- MAGIC 1. Selecione uma conversa para visitá-la novamente. A partir daqui, você pode rever a conversa ou até mesmo continuá-la. Por exemplo, vamos enviar um novo prompt: *Show me the above data as a bar chart.*<br>
-- MAGIC    ![Add a visualization](images/genie/showbarchart.png)
-- MAGIC
-- MAGIC Como podemos ver, uma conversa pode ser continuada, já que Genie se lembra do contexto da conversa que você reabriu.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
-- MAGIC
