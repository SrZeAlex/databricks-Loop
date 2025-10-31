-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2.3 Laboratório - Importar dados para o Databricks
-- MAGIC ### Duração: ~15 minutos
-- MAGIC
-- MAGIC Neste laboratório, você ingerirá uma série de arquivos JSON e criará uma tabela (tabela Delta) a partir de um volume do Databricks.
-- MAGIC
-- MAGIC ### Objetivos
-- MAGIC - Demonstrar como criar uma tabela ingerindo vários arquivos JSON brutos armazenados em um volume do Databricks usando uma `COPY INTO` ou outra função `read_files`.

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
-- MAGIC    b. Se você não tiver um **SQL shared_warehouse** em **recursos recentes**, conclua o seguinte:
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
-- MAGIC Lembre-se de que a configuração do laboratório é criada com o [0 - OBRIGATÓRIO - Configuração do curso e descobrimento de dados]($../0 - REQUIRED - Course Setup and Data Discovery). Se você encerrar a sessão de laboratório ou se a sessão atingir o limite de tempo, seu ambiente será redefinido e você precisará executar novamente a Configuração do curso do Notebook.

-- COMMAND ----------

-- MAGIC %run ../Includes/2.3-Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Cenário de laboratório
-- MAGIC
-- MAGIC Você é analista da DB Inc., e recebeu a tarefa de ingerir arquivos JSON em uma tabela. A empresa colocou vários arquivos JSON no volume **dbacademy_retail.v01.retail-pipeline.customers.stream_json**, e seu trabalho é ler esses arquivos JSON em uma tabela estruturada para análise posterior.
-- MAGIC
-- MAGIC Siga os passos abaixo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Comece exibindo seu catálogo e esquema default. Confirme se o catálogo atual é **samples** e se o esquema atual é **nyctaxi**.

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Modifique o catálogo e o esquema default para o seguinte:
-- MAGIC      - Defina **databrickstheloop** como o catálogo default.
-- MAGIC      - Defina seu esquema **username** como o esquema default. Ao definir o esquema default, use a cláusula `IDENTIFIER` com a variável `DA.schema_name` para definir seu esquema.
-- MAGIC
-- MAGIC     Execute a célula e confirme se o catálogo e o esquema default foram modificados.
-- MAGIC

-- COMMAND ----------

<FILL-IN>


---- Execute abaixo para confirmar se o catálogo e o esquema default foram modificados
SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Exiba todos os arquivos JSON no volume **/Volumes/dbacademy_retail/v01/retail-pipeline/customers/stream_json/** programaticamente. Confirme se o volume contém 31 arquivos JSON.
-- MAGIC
-- MAGIC **DICAS:**
-- MAGIC   - Primeiro, navegue até o volume usando a barra de navegação à direita para visualizar manualmente os arquivos.
-- MAGIC   - Em seguida, use a [instrução LIST](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-list)  para listar programaticamente todos os arquivos JSON no volume. Você pode inserir o caminho para o volume usando a interface do usuário.

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Query dos arquivos JSON para visualizar os dados JSON brutos. Essa é uma boa maneira de ver exatamente como os arquivos JSON são estruturados. Observe que cada arquivo JSON contém uma lista de dicionários, onde cada dicionário representa uma linha de dados.
-- MAGIC
-- MAGIC **DICAS:**
-- MAGIC   - [Query dos dados por caminho](https://docs.databricks.com/aws/en/query#query-data-by-path)  para visualizar os arquivos JSON brutos usando palavra-chave `text`.  
-- MAGIC   - O caminho para o volume é `/Volumes/dbacademy_retail/v01/retail-pipeline/customers/stream_json/`. Certifique-se de cercar o caminho do volume em crases.
-- MAGIC   - Adicione uma opção `LIMIT` para limitar os resultados a 10 linhas.
-- MAGIC   - Você também pode download um dos arquivos JSON do volume e abri-lo em seu editor favorito como outra abordagem para visualizar os arquivos brutos.
-- MAGIC
-- MAGIC
-- MAGIC **Pequeno exemplo do arquivo JSON:**
-- MAGIC ```JSON
-- MAGIC [
-- MAGIC     {
-- MAGIC         "name": "Brent Chavez",
-- MAGIC         "email": "nelsonjoy@example.com",
-- MAGIC         "address": "568 David Brook Apt. 524",
-- MAGIC         "city": "Norwalk",
-- MAGIC         "state": "CA",
-- MAGIC         "zip_code": "45049",
-- MAGIC         "operation": "NEW",
-- MAGIC         "timestamp": 1632417981,
-- MAGIC         "customer_id": 23056
-- MAGIC     },
-- MAGIC     {
-- MAGIC         "name": "James Cruz",
-- MAGIC         "email": "perkinsdeborah@example.net",
-- MAGIC         "address": "741 Wendy Plains Apt. 143",
-- MAGIC         "city": "San Francisco",
-- MAGIC         "state": "CA",
-- MAGIC         "zip_code": "42872",
-- MAGIC         "operation": "NEW",
-- MAGIC         "timestamp": 1632421305,
-- MAGIC         "customer_id": 23057
-- MAGIC     },
-- MAGIC     ...
-- MAGIC ]
-- MAGIC ```

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Para obter uma exibição tabular dos arquivos JSON, query os arquivos usando `SELECT * FROM json.`. Isso ajudará você a explorar a aparência dos arquivos JSON como uma tabela usando os argumentos default. Dependendo da estrutura do arquivo JSON, isso será lido como uma tabela de forma diferente.
-- MAGIC
-- MAGIC     **DICAS:**
-- MAGIC       - [query dos dados por caminho](https://docs.databricks.com/aws/en/query#query-data-by-path)  para visualizar os arquivos JSON brutos usando `json`.
-- MAGIC       - Adicione uma opção `LIMIT` para limitar os resultados a 10 linhas.

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Agora que você explorou os arquivos JSON, seu objetivo é:
-- MAGIC     - Crie uma tabela chamada **customers_lab** no esquema **databrickstheloop.username**.
-- MAGIC
-- MAGIC     - Use os arquivos JSON em **/Volumes/dbacademy_retail/v01/retail-pipeline/customers/stream_json/**.
-- MAGIC
-- MAGIC     - Você pode usar a instrução `COPY INTO` ou a função `read_files`.
-- MAGIC
-- MAGIC <br></br>
-- MAGIC **DICAS:**
-- MAGIC - [COPY INTO](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into)
-- MAGIC   - Com `COPY INTO`, você precisará criar uma tabela primeiro.
-- MAGIC     - Você pode criar o esquema para a tabela.
-- MAGIC     - Se você criar a tabela sem um esquema, veja a opção `COPY_OPTIONS ('mergeSchema' = 'true')` para mesclar o esquema quando adicionado à tabela.
-- MAGIC
-- MAGIC - [função read_files com valor de tabela](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files)
-- MAGIC

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Execute a query a seguir para exibir sua tabela. Confirme o seguinte:
-- MAGIC    - A tabela **customers_lab** contém 1.467 linhas
-- MAGIC    - A tabela contém as colunas: **address, city, customer_id, email, name, operation, state, timestamp, zip_code**

-- COMMAND ----------

SELECT *
FROM customers_lab;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
