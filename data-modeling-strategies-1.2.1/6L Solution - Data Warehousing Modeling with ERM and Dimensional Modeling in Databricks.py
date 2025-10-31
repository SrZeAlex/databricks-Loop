# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Modelagem de Data Warehousing com ERM e Modelagem Dimensional no Databricks
# MAGIC Neste laboratório, você explorará técnicas modernas de data warehousing usando Databricks, começando com Entity Relationship Modeling \(ERM\) e Dimensional Modeling. Você também se aprofundará em abordagens avançadas, como o Data Vault 2.0 e a engenharia de recursos de machine learning com o Repositório de recursos de Databricks. Desde a definição de restrições relacionais até o acompanhamento de alterações históricas com o SCD Tipo 2, o design de modelos de vault e a execução de inferências em lote, este laboratório equipa você para criar arquiteturas de dados escalonáveis e prontas para análise e fluxos de trabalho de ML em uma plataforma unificada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final deste laboratório, você será capaz de:
# MAGIC * Aplicar restrições de primary e foreign key para manter a integridade relacional em tabelas Delta
# MAGIC * Construir modelos dimensionais usando Slowly Changing Dimension (SCD) Tipo 2
# MAGIC * Implementar componentes do Data Vault 2.0 (Hubs, Links, Satélites) usando hash keys
# MAGIC * Projetar pipelines ETL escaláveis para preencher e gerenciar esquemas relacionais, dimensionais e de Vault
# MAGIC * Criar e gerenciar tabelas de recursos usando o repositório de recursos de Databricks
# MAGIC * Treinar modelos de machine learning usando recursos registrados para pipelines reproduzíveis
# MAGIC * Realizar inferência em lote, unindo dados de recursos e aplicando modelos treinados em escala

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨OBRIGATÓRIO - SELECT CLASSIC COMPUTE
# MAGIC Antes de executar células neste notebook, selecione seu cluster de compute clássico no laboratório. Lembre-se de que **Serverless** está habilitado por default.
# MAGIC
# MAGIC Siga estas etapas para selecionar o cluster de compute clássico:
# MAGIC * Navegue até o canto superior direito deste notebook e clique no menu dropdown para selecionar seu cluster. Por default, o notebook usará **Serverless**. <br>
# MAGIC
# MAGIC ##### **📌**Se o cluster estiver disponível, selecione-o e continue para a próxima célula. Se o cluster não for mostrado:
# MAGIC   - Na lista dropdown, selecione **More**.
# MAGIC   - No pop-up **Attach to an existing compute resource**, selecione a primeira dropdown. Você verá um nome de cluster exclusivo nessa dropdown. Selecione esse cluster.
# MAGIC
# MAGIC **NOTA:** Se o cluster tiver sido encerrado, talvez seja necessário reiniciá-lo para selecioná-lo. Para fazer isso:
# MAGIC 1. Clique com o botão direito do rato em **Compute** no painel de navegação esquerdo e selecione *Open in new tab*.
# MAGIC 2. Localize o ícone de triângulo à direita do nome do cluster de compute e clique nele.
# MAGIC 3. Aguarde alguns minutos para que o cluster seja iniciado.
# MAGIC 4. Quando o cluster estiver em execução, conclua as etapas acima para selecioná-lo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requisitos
# MAGIC
# MAGIC Analise os seguintes requisitos antes de iniciar a demonstração:
# MAGIC
# MAGIC * Para executar este notebook, você precisa usar um dos seguintes Databricks runtime(s): **15.4.x-scala2.12**
# MAGIC * Como alternativa, em um cluster de tempo de execução não-ML, instale manualmente as bibliotecas necessárias de maneira semelhante. (Para esta demonstração, temos um cluster não-ML)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 1: Instale as bibliotecas necessárias e execute o script de instalação
# MAGIC
# MAGIC **Tarefa 1:** Instale a biblioteca do Engenharia de recursos da Databricks para habilitar definições de tabela, criação de conjunto de treinamento e publicação de recursos.

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

# MAGIC %md
# MAGIC Uma vez que a biblioteca é instalada, reiniciamos o Python kernel para que ele esteja totalmente disponível.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC **Tarefa 2:** Antes de prosseguir com o conteúdo desta e de outras atividades do laboratório, certifique-se de ter executado o script de configuração do laboratório.
# MAGIC Como resultado da execução do script, você criará:
# MAGIC 1. Um catálogo dedicado com o nome da sua conta de usuário de laboratório.  
# MAGIC 2. Esquemas nomeados `bronze`, `silver` e `gold` dentro do catálogo.  
# MAGIC 3. Tabelas TPC-H copiadas de Amostras para a esquema `bronze`.

# COMMAND ----------

# MAGIC %run ./Includes/setup/lab_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 2: Escolha o catálogo e a esquema de trabalho
# MAGIC
# MAGIC Ao longo deste laboratório, você pode criar novas tabelas em `silver` (ou outra esquema de sua escolha). 
# MAGIC Para demonstração, usaremos a esquema `silver` no catálogo específico do usuário.

# COMMAND ----------

import re
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

# Obter o usuário atual e extrair o nome do catálogo dividindo o e-mail em '@' e tomando a primeira parte
user_id = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]

# Substitua todos os caracteres especiais no `user_id` por um sublinhado '_' para criar o nome do catálogo
catalog_name = re.sub(r'[^a-zA-Z0-9]', '_', user_id) # Código novo

# Defina o nome da esquema prata a ser usado
silver_schema = "silver"
gold_schema = "gold"

print("Catalog and schemas set for feature development.")

# COMMAND ----------

# Criar um widget para capturar o nome do catálogo e todos os nomes de esquema
dbutils.widgets.text("catalog_name", catalog_name)
dbutils.widgets.text("silver_schema", silver_schema)
dbutils.widgets.text("gold_schema", gold_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o catálogo atual para o nome do catálogo extraído
# MAGIC USE CATALOG IDENTIFIER(:catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o esquema atual para o nome do esquema definido
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os nomes default do catálogo e do esquema
# MAGIC SELECT current_catalog() AS Catalog_Name, current_schema() AS Schema_Name;

# COMMAND ----------

# Definir o catálogo e o esquema default no spark sql
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {silver_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Passo 3: Criar tabelas com restrições**  
# MAGIC
# MAGIC **Tarefa 1:**  
# MAGIC Crie duas tabelas com restrições de **Primary Key (PK) e Foreign Key (FK)**:  
# MAGIC
# MAGIC 1. **Create `lab_customer`** com uma primary key em `c_custkey`.  
# MAGIC 2. **Create `lab_orders`** com:  
# MAGIC    - Uma primary key em `o_orderkey`.  
# MAGIC    - Uma foreign key em `o_custkey` referenciando `lab_customer(c_custkey)`.  
# MAGIC
# MAGIC **Nota:**  
# MAGIC - A Databricks não impõe restrições PK/FK, mas as usa para relacionamentos em **Entity Relationship Diagrams (ERDs)** no Catalog Explorer.  
# MAGIC - As restrições de `NOT NULL` e `CHECK` são aplicadas.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela lab_customer com uma restrição PRIMARY KEY no c_custkey
# MAGIC CREATE TABLE IF NOT EXISTS lab_customer 
# MAGIC (
# MAGIC   c_custkey INT,
# MAGIC   c_name STRING,
# MAGIC   c_address STRING,
# MAGIC   c_nationkey INT,
# MAGIC   c_phone STRING,
# MAGIC   c_acctbal DECIMAL(12,2),
# MAGIC   c_mktsegment STRING,
# MAGIC   c_comment STRING,
# MAGIC   CONSTRAINT pk_custkey PRIMARY KEY (c_custkey)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Verifique o catálogo e a esquema atuais.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verificar catálogo atual
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verificar o esquema atual
# MAGIC SELECT current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC **Tarefa 2:**  
# MAGIC Crie a tabela `lab_orders` com as seguintes restrições:  
# MAGIC    - Uma **Primary Key (PK)** em `o_orderkey`.  
# MAGIC    - Uma **Foreign Key (FK)** na `o_custkey` referenciando `lab_customer(c_custkey)`.
# MAGIC
# MAGIC **Nota:**  
# MAGIC    - Use o nome do catálogo default real para a palavra-chave `REFERENCES`.
# MAGIC
# MAGIC **Exemplo de linha de código de destino:**  
# MAGIC ```dbsql
# MAGIC CONSTRAINT fk_custkey FOREIGN KEY (o_custkey) REFERENCES <default_catalog_name>.silver.lab_customer
# MAGIC ```
# MAGIC
# MAGIC Neste exemplo, substitua o valor '<default_catalog_name>' pelo valor real da saída da query anterior.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela lab_orders com PRIMARY KEY no o_orderkey
# MAGIC ---- e uma FOREIGN KEY referenciando lab_customer(c_custkey)
# MAGIC ---- Nota: Forneça REFERENCES com namespace de três níveis
# MAGIC CREATE TABLE IF NOT EXISTS lab_orders
# MAGIC (
# MAGIC   o_orderkey INT,
# MAGIC   o_custkey INT,
# MAGIC   o_orderstatus STRING,
# MAGIC   o_totalprice DECIMAL(12,2),
# MAGIC   o_orderdate DATE,
# MAGIC   o_orderpriority STRING,
# MAGIC   o_clerk STRING,
# MAGIC   o_shippriority INT,
# MAGIC   o_comment STRING,
# MAGIC   CONSTRAINT pk_orderkey PRIMARY KEY (o_orderkey),
# MAGIC   CONSTRAINT fk_custkey FOREIGN KEY (o_custkey) REFERENCES <default_catalog_name>.silver.lab_customer(c_custkey)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ###Passo 4: Inserir dados das tabelas bronze TPC-H
# MAGIC
# MAGIC **Tarefa:**  
# MAGIC Preencha as tabelas recém-criadas `lab_customer` e `lab_orders` com dados das tabelas TPC-H localizadas na esquema `bronze`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Inserir dados no lab_customer de bronze.customer
# MAGIC INSERT INTO lab_customer
# MAGIC SELECT
# MAGIC   c_custkey,
# MAGIC   c_name,
# MAGIC   c_address,
# MAGIC   c_nationkey,
# MAGIC   c_phone,
# MAGIC   c_acctbal,
# MAGIC   c_mktsegment,
# MAGIC   c_comment
# MAGIC FROM bronze.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Inserir dados no lab_orders de bronze.orders
# MAGIC INSERT INTO lab_orders
# MAGIC SELECT
# MAGIC   o_orderkey,
# MAGIC   o_custkey,
# MAGIC   o_orderstatus,
# MAGIC   o_totalprice,
# MAGIC   o_orderdate,
# MAGIC   o_orderpriority,
# MAGIC   o_clerk,
# MAGIC   o_shippriority,
# MAGIC   o_comment
# MAGIC FROM bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Passo 5: Demonstrar violações `CONSTRAINT`
# MAGIC
# MAGIC **Tarefa:** Validar comportamento de restrição  
# MAGIC
# MAGIC Como a Databricks não impõe restrições de primary e foreign key, execute as seguintes etapas para observar seu comportamento:  
# MAGIC
# MAGIC 1. **Foreign Key Test:** Insira uma linha em `lab_orders` com um `o_custkey` que não existe em `lab_customer`.  
# MAGIC 2. **Primary Key Test:** Insira uma linha duplicada em `lab_customer` usando um `c_custkey` já existente.  
# MAGIC
# MAGIC Analise os resultados para confirmar que essas operações são bem-sucedidas sem imposição de restrições.

# COMMAND ----------

# MAGIC %md
# MAGIC **Tarefa 1:** Testar restrição de Foreign Key  
# MAGIC
# MAGIC Insira uma linha em `lab_orders` com uma `o_custkey` que não existe em `lab_customer`. Como a Databricks não impõe restrições de foreign key, verifique se a inserção é bem-sucedida sem erros.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Teste de Restrição de Foreign Key
# MAGIC INSERT INTO lab_orders 
# MAGIC VALUES
# MAGIC (
# MAGIC   9999999,         -- o_orderkey
# MAGIC   9999999,         -- o_custkey (inexistente em lab_customer)
# MAGIC   'F',
# MAGIC   1000.00,
# MAGIC   current_date(),
# MAGIC   '3-LOW',
# MAGIC   'Clerk#000000001',
# MAGIC   0,
# MAGIC   'Testing invalid customer key'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Tarefa 2:** Testar restrição de Primary Key  
# MAGIC
# MAGIC Insira uma linha duplicada em `lab_customer` usando um valor `c_custkey` que já existe na tabela. Como a Databricks não impõe restrições de primary key, verifique se a inserção é permitida sem erros.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Teste de restrição de Primary Key ----
# MAGIC INSERT INTO lab_customer
# MAGIC VALUES
# MAGIC (
# MAGIC   1,
# MAGIC   'Duplicate Customer',
# MAGIC   'Duplicate Address',
# MAGIC   9999,
# MAGIC   '999-999-9999',
# MAGIC   9999.99,
# MAGIC   'DUPLICATE_SEGMENT',
# MAGIC   'Inserting a duplicate primary key for demonstration'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 6: Reverter para um estado limpo
# MAGIC
# MAGIC **Tarefa:** Reverter para um estado limpo  
# MAGIC
# MAGIC Remova as linhas violadoras de `lab_customer` e `lab_orders` para restaurar um estado limpo. Escolha uma das seguintes abordagens:  
# MAGIC
# MAGIC - **Delete specific rows**: Remova manualmente as linhas violadoras adicionadas recentemente especificando suas keys.  
# MAGIC - **Truncate tables**: Limpe todos os dados, mantendo a estrutura da tabela intacta.  
# MAGIC - **Use Viagem do Tempo Delta**: Reverta a tabela para uma versão anterior antes que as violações de restrição ocorressem.  
# MAGIC
# MAGIC Certifique-se de ajustar as keys de acordo se valores diferentes foram inseridos.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Remover a violação de foreign key (orderkey=9999999)
# MAGIC DELETE FROM lab_orders
# MAGIC WHERE o_orderkey = 9999999;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Remover a linha de primary key duplicada
# MAGIC DELETE FROM lab_customer
# MAGIC WHERE c_custkey = 1 
# MAGIC   AND c_name = 'Duplicate Customer';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 7: Visualizando o diagrama de ER na Databricks
# MAGIC
# MAGIC **Tarefa:** Visualizar o diagrama de ER na Databricks  
# MAGIC
# MAGIC Use a Databricks para explorar as relações entre suas tabelas:  
# MAGIC
# MAGIC 1. Abra **Databricks** e navegue até o **Catalog Explorer** no painel esquerdo.  
# MAGIC 2. Selecione seu **catalog** atribuído e abra o **schema** (por exemplo, `silver`).  
# MAGIC 3. Localize a tabela `lab_orders`, que contém uma foreign key de referência `lab_customer`.  
# MAGIC 4. Clique em **View Relationships** para visualizar o **Entity Relationship (ER) Diagram** mostrando a conexão entre `lab_orders` e `lab_customer`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 8: Definições de tabela
# MAGIC
# MAGIC **Tarefa 1:** Definir tabelas Prata
# MAGIC
# MAGIC Crie **refined tables** no **silver schema** para padronizar e limpar os dados.  
# MAGIC
# MAGIC 1. **Define the `refined_customer` table** com base na tabela TPC-H `customer` .  
# MAGIC 2. **Define the `refined_orders` table** com base na tabela TPC-H `orders` .  
# MAGIC 3. **Standardize column names** para manter a consistência.  
# MAGIC
# MAGIC Essas tabelas serão usadas na próxima etapa para carregamento de dados.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Defina o esquema atual para o nome do silver_schema extraído no DBSQL
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema);

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela refined_customer se ela ainda não existir
# MAGIC CREATE TABLE IF NOT EXISTS refined_customer (
# MAGIC   customer_id INT,            -- Identificador exclusivo para o cliente
# MAGIC   name STRING,                -- Nome do cliente
# MAGIC   address STRING,             -- Endereço do cliente
# MAGIC   nation_key INT,             -- Foreign key ligando à tabela de nação
# MAGIC   phone STRING,               -- Número de telefone do cliente
# MAGIC   acct_bal DECIMAL(12, 2),    -- Saldo da conta do cliente
# MAGIC   market_segment STRING,      -- Segmento de mercado do cliente
# MAGIC   comment STRING              -- Comentários adicionais sobre o cliente
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela refined_orders se ela ainda não existir
# MAGIC CREATE TABLE IF NOT EXISTS refined_orders (
# MAGIC   order_id INT,                -- Identificador exclusivo para o pedido
# MAGIC   customer_id INT,             -- Foreign key ligando à tabela do cliente
# MAGIC   order_status STRING,         -- Estado do pedido (por exemplo, pendente, enviado)
# MAGIC   total_price DECIMAL(12, 2),  -- Preço total do pedido
# MAGIC   order_date DATE,             -- Data em que o pedido foi feito
# MAGIC   order_priority STRING,       -- Nível de prioridade do pedido
# MAGIC   clerk STRING,                -- Escriturário que cuidou do pedido
# MAGIC   ship_priority INT,           -- Prioridade de envio do pedido
# MAGIC   comment STRING               -- Comentários adicionais sobre o pedido
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Tarefa 2: Criar tabelas Ouro (Star Schema)**  
# MAGIC
# MAGIC Defina as **gold tables** usando um **star schema** para análises e relatórios.  
# MAGIC
# MAGIC 1. **Create `DimCustomer`** com atributos SCD (Slowly Changing Dimension) Tipo 2, incluindo:  
# MAGIC    - `start_date`, `end_date`, e `is_current` para rastreamento histórico.  
# MAGIC    - `GENERATED ALWAYS AS IDENTITY` para keys de substituição.  
# MAGIC
# MAGIC 2. **Create `DimDate`** para armazenar atributos relacionados à data para análise.  
# MAGIC
# MAGIC 3. **Create `FactOrders`** como a tabela de fatos central, vinculando-se a tabelas de dimensões.  
# MAGIC
# MAGIC Essas tabelas serão usadas para otimizar o desempenho da query e oferecer suporte ao controle histórico.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Definir o esquema atual para o nome do gold_schema extraído no DBSQL
# MAGIC USE SCHEMA IDENTIFIER(:gold_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela DimCustomer para armazenar detalhes do cliente com atributos Slowly Changing Dimension (SCD) Tipo 2 para rastreamento histórico
# MAGIC CREATE TABLE IF NOT EXISTS DimCustomer
# MAGIC (
# MAGIC   dim_customer_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Key substituta para a tabela de dimensões
# MAGIC   customer_id INT,                                       -- Identificador exclusivo para o cliente
# MAGIC   name STRING,                                           -- Nome do cliente
# MAGIC   address STRING,                                        -- Endereço do cliente
# MAGIC   nation_key INT,                                        -- Foreign key ligando à tabela de nação
# MAGIC   phone STRING,                                          -- Número de telefone do cliente
# MAGIC   acct_bal DECIMAL(12,2),                                -- Saldo da conta do cliente
# MAGIC   market_segment STRING,                                 -- Segmento de mercado do cliente
# MAGIC   comment STRING,                                        -- Comentários adicionais sobre o cliente
# MAGIC   start_date DATE,                                       -- Data de início do SCD2 indicando o início da validade do registro
# MAGIC   end_date DATE,                                         -- Data de término do SCD2 indicando o fim da validade do registro
# MAGIC   is_current BOOLEAN,                                    -- Sinalizador para indicar se o registro é a versão atual
# MAGIC   CONSTRAINT pk_dim_customer PRIMARY KEY (dim_customer_key)  -- Restrição de Primary key na key substituta
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela Simple DimDate para armazenar informações relacionadas à data
# MAGIC CREATE TABLE IF NOT EXISTS DimDate (
# MAGIC   dim_date_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Key substituta para a tabela DimDate
# MAGIC   full_date DATE,                                    -- Valor de data completa
# MAGIC   day INT,                                           -- Dia do mês
# MAGIC   month INT,                                         -- Mês do ano
# MAGIC   year INT,                                          -- Valor do ano
# MAGIC   CONSTRAINT pk_dim_date PRIMARY KEY (dim_date_key) RELY  -- Restrição de Primary key no dim_date_key
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verificar catálogo atual
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verificar o esquema atual
# MAGIC SELECT current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC **Tarefa 3:**  
# MAGIC Crie a tabela `FactOrders` referenciando as tabelas `DimCustomer` e `DimDate`.
# MAGIC
# MAGIC **Nota:**  
# MAGIC    - Use o nome do catálogo default real para a palavra-chave `REFERENCES`.
# MAGIC
# MAGIC **Exemplo de linha de código de destino:**  
# MAGIC ```dbsql
# MAGIC CONSTRAINT fk_customer FOREIGN KEY (dim_customer_key) REFERENCES <default_catalog_name>.gold.DimCustomer(dim_customer_key),  -- Restrição de Foreign key vinculando ao DimCustomer
# MAGIC
# MAGIC CONSTRAINT fk_date FOREIGN KEY (dim_date_key) REFERENCES <default_catalog_name>.gold.DimDate(dim_date_key)  -- Restrição de Foreign key vinculando a DimDate
# MAGIC ```
# MAGIC
# MAGIC Neste exemplo, substitua o valor '<default_catalog_name>' pelo valor real da saída da query anterior.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela FactOrders fazendo referência às tabelas DimCustomer e DimDate
# MAGIC CREATE TABLE IF NOT EXISTS FactOrders (
# MAGIC   fact_orders_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Key substituta para a tabela FactOrders
# MAGIC   order_id INT,                                        -- Identificador exclusivo para o pedido
# MAGIC   dim_customer_key BIGINT,                             -- Foreign key vinculando à tabela DimCustomer
# MAGIC   dim_date_key BIGINT,                                 -- Foreign key vinculando à tabela DimDate
# MAGIC   total_price DECIMAL(12, 2),                          -- Preço total do pedido
# MAGIC   order_status STRING,                                 -- Estado do pedido (por exemplo, pendente, enviado)
# MAGIC   order_priority STRING,                               -- Nível de prioridade do pedido
# MAGIC   clerk STRING,                                        -- Escriturário que cuidou do pedido
# MAGIC   ship_priority INT,                                   -- Prioridade de envio do pedido
# MAGIC   comment STRING,                                      -- Comentários adicionais sobre o pedido
# MAGIC   CONSTRAINT pk_fact_orders PRIMARY KEY (fact_orders_key),  -- Restrição de Primary key no fact_orders_key
# MAGIC   CONSTRAINT fk_customer FOREIGN KEY (dim_customer_key) REFERENCES <default_catalog_name>.gold.DimCustomer(dim_customer_key),  -- Restrição de Foreign key vinculando ao DimCustomer
# MAGIC   CONSTRAINT fk_date FOREIGN KEY (dim_date_key) REFERENCES <default_catalog_name>.gold.DimDate(dim_date_key)  -- Restrição de Foreign key vinculando a DimDate
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notas sobre `GENERATED ALWAYS AS IDENTITY`
# MAGIC - Cada tabela gera automaticamente números exclusivos para a coluna da key substituta.  
# MAGIC - Você não insere um valor para essas colunas; A Delta lida com isso perfeitamente.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 9: Carregar dados na Prata
# MAGIC
# MAGIC **Tarefa:** Carregue dados das tabelas TPC-H `bronze` (`bronze.customer` e `bronze.orders`) para as tabelas recém-criadas `refined_customer` e `refined_orders` no esquema **silver**.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Use a instrução `SELECT` para extrair dados de `bronze.customer` e `bronze.orders`.
# MAGIC 2. Insira os dados nas tabelas respectivas **silver**: `refined_customer` e `refined_orders`.
# MAGIC
# MAGIC Certifique-se de que os dados sejam limpos e transformados conforme necessário antes de serem carregados nas tabelas pratas.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Alternar para o catálogo usando o nome do catálogo extraído
# MAGIC USE CATALOG IDENTIFIER(:catalog_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Alternar para o esquema prata
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema);

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Inserir dados transformados da tabela bronze.customer na tabela refined_customer
# MAGIC INSERT INTO
# MAGIC   refined_customer
# MAGIC SELECT
# MAGIC   c_custkey AS customer_id,                      -- Identificador exclusivo para o cliente
# MAGIC   TRIM(c_name) AS name,                          -- Nome do cliente
# MAGIC   TRIM(c_address) AS address,                    -- Endereço do cliente
# MAGIC   c_nationkey AS nation_key,                     -- Foreign key ligando à tabela de nação
# MAGIC   TRIM(c_phone) AS phone,                        -- Número de telefone do cliente
# MAGIC   CAST(c_acctbal AS DECIMAL(12, 2)) AS acct_bal, -- Saldo da conta do cliente
# MAGIC   TRIM(c_mktsegment) AS market_segment,          -- Segmento de mercado do cliente
# MAGIC   TRIM(c_comment) AS comment                     -- Comentários adicionais sobre o cliente
# MAGIC FROM
# MAGIC   bronze.customer;                               -- Tabela de origem na camada bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Inserir dados transformados da tabela bronze.orders na tabela refined_orders
# MAGIC INSERT INTO
# MAGIC   refined_orders
# MAGIC SELECT
# MAGIC   o_orderkey AS order_id,                      -- Identificador exclusivo para o pedido
# MAGIC   o_custkey AS customer_id,                    -- Foreign key ligando à tabela do cliente
# MAGIC   TRIM(o_orderstatus) AS order_status,         -- Estado do pedido (por exemplo, pendente, enviado)
# MAGIC   CAST(o_totalprice AS DECIMAL(12, 2)) AS total_price,  -- Preço total do pedido
# MAGIC   o_orderdate AS order_date,                   -- Data em que o pedido foi feito
# MAGIC   TRIM(o_orderpriority) AS order_priority,     -- Nível de prioridade do pedido
# MAGIC   TRIM(o_clerk) AS clerk,                      -- Escriturário que cuidou do pedido
# MAGIC   o_shippriority AS ship_priority,             -- Prioridade de envio do pedido
# MAGIC   TRIM(o_comment) AS comment                   -- Comentários adicionais sobre o pedido
# MAGIC FROM
# MAGIC   bronze.orders;                               -- Tabela de origem na camada bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 10: Validar os registros da tabela
# MAGIC **Tarefa:** Validar se os registros foram carregados com êxito nas tabelas `refined_customer` e `refined_orders`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela refined_customer
# MAGIC SELECT COUNT(*) AS refined_customer_count FROM refined_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela refined_orders
# MAGIC SELECT COUNT(*) AS refined_orders_count FROM refined_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela refined_customer
# MAGIC SELECT COUNT(*) AS refined_customer_count FROM refined_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela refined_orders
# MAGIC SELECT COUNT(*) AS refined_orders_count FROM refined_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 11: Carga Inicial em Ouro (Modelo Dimensional)
# MAGIC
# MAGIC **Tarefa:** Execute a carga inicial de `DimCustomer`, `DimDate` e `FactOrders` na esquema **gold**.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Carregue todos os dados do cliente na tabela `DimCustomer` como entradas *current*.
# MAGIC    - Use uma instrução `SELECT` para extrair dados do cliente da fonte refinada e inseri-los em `DimCustomer` como clientes atuais.
# MAGIC    
# MAGIC 2. Crie e insira entradas de data na tabela `DimDate` a partir dos dados `refined_orders`.
# MAGIC    - Se a tabela `DimDate` estiver pré-carregada com datas diárias por vários anos, certifique-se de que todas as datas relevantes sejam preenchidas em `refined_orders` a partir de `DimDate`.
# MAGIC
# MAGIC 3. Preencha a tabela `FactOrders` vinculando cada pedido às keys de dimensão corretas (por exemplo, cliente, data).
# MAGIC    - Use o `JOIN` apropriado para vincular a tabela `refined_orders` com `DimCustomer` e `DimDate` e inserir os dados em `FactOrders`.
# MAGIC
# MAGIC Verifique se todas as keys de dimensão estão vinculadas corretamente e se as transformações necessárias foram aplicadas antes de carregar nas tabelas ouro.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Alternar para o esquema ouro usando o comando USE SCHEMA SQL
# MAGIC USE SCHEMA IDENTIFIER(:gold_schema);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.1 Carga inicial do DimCustomer (SCD Tipo 2)
# MAGIC
# MAGIC **Tarefa:** Executar a carga inicial de `DimCustomer` com a lógica SCD Tipo 2.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Atualize todas as linhas da tabela `DimCustomer` com `start_date = CURRENT_DATE()`, `end_date = NULL`, e `is_current = TRUE` para todos os clientes.
# MAGIC    - Use uma instrução `UPDATE` para definir esses valores para cada linha do cliente `DimCustomer`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Inserir dados na tabela de dimensão DimCustomer
# MAGIC INSERT INTO DimCustomer
# MAGIC (
# MAGIC   customer_id,    -- Identificador exclusivo para o cliente
# MAGIC   name,           -- Nome do cliente
# MAGIC   address,        -- Endereço do cliente
# MAGIC   nation_key,     -- Key que representa a nação do cliente
# MAGIC   phone,          -- Número de telefone do cliente
# MAGIC   acct_bal,       -- Saldo da conta do cliente
# MAGIC   market_segment, -- Segmento de mercado do cliente
# MAGIC   comment,        -- Comentários adicionais sobre o cliente
# MAGIC   start_date,     -- Data de início do registro
# MAGIC   end_date,       -- Data de término do registro (NULL para registros atuais)
# MAGIC   is_current      -- Sinalizador indicando se o registro é atual
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_id,    -- Selecione customer_id na tabela de origem
# MAGIC   name,           -- Selecione o nome na tabela de origem
# MAGIC   address,        -- Selecione o endereço na tabela de origem
# MAGIC   nation_key,     -- Selecione nation_key na tabela de origem
# MAGIC   phone,          -- Selecione o telefone na tabela de origem
# MAGIC   acct_bal,       -- Selecione acct_bal na tabela de origem
# MAGIC   market_segment, -- Selecione market_segment na tabela de origem
# MAGIC   `comment`, -- Selecionar comentário da tabela de origem
# MAGIC   CURRENT_DATE(), -- Definir start_date para a data atual
# MAGIC   NULL,           -- Defina end_date como NULL para registros atuais
# MAGIC   TRUE            -- Defina is_current como TRUE para registros atuais
# MAGIC FROM IDENTIFIER(:silver_schema || '.' || 'refined_customer')   -- Tabela de origem no esquema prata

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.2 DimDate
# MAGIC
# MAGIC **Tarefa:** Preencher a tabela `DimDate` com valores exclusivos `order_date` de `refined_orders`.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Extraia valores exclusivos `order_date` de `refined_orders`.
# MAGIC    - Use uma query `SELECT DISTINCT order_date` para recuperar as datas exclusivas.
# MAGIC
# MAGIC 2. Use funções internas para dividir os valores `order_date` em componentes `day`, `month` e `year`.
# MAGIC    - Aplicar funções de data para extrair `day`, `month` e `year` de `order_date`.
# MAGIC
# MAGIC 3. Insira os valores de data dividida na tabela `DimDate`.
# MAGIC    - Use uma query `INSERT INTO DimDate` com os componentes de data transformados.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Inserir datas distintas na tabela de dimensão DimDate
# MAGIC INSERT INTO DimDate
# MAGIC (
# MAGIC   full_date,  -- Valor de data completa
# MAGIC   day,        -- Parte do dia da data
# MAGIC   month,      -- Parte do mês da data
# MAGIC   year        -- Parte do ano da data
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC   order_date,          -- Valor de data completo de refined_orders
# MAGIC   DAY(order_date),     -- Parte do dia extraída da data
# MAGIC   MONTH(order_date),   -- Parte do mês extraída da data
# MAGIC   YEAR(order_date)     -- Parte do ano extraída da data
# MAGIC FROM IDENTIFIER(:silver_schema || '.' || 'refined_orders')
# MAGIC WHERE order_date IS NOT NULL  -- Verifique se a data não é nula

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.3 FactOrders
# MAGIC
# MAGIC **Tarefa:** Preencha a tabela `FactOrders` vinculando cada ordem a `DimCustomer` e `DimDate`.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Una `refined_orders` com `DimCustomer` usando `(customer_id = dc.customer_id AND is_current = TRUE)` para garantir que apenas clientes ativos estejam vinculados.
# MAGIC    - Use um `JOIN` entre `refined_orders` e `DimCustomer` em `customer_id` e filtre com `is_current = TRUE`.
# MAGIC
# MAGIC 2. Vincule cada pedido a `DimDate` com base em `order_date`.
# MAGIC    - Use um `JOIN` entre `refined_orders` e `DimDate` em `order_date`.
# MAGIC
# MAGIC 3. Insira os dados unidos na tabela `FactOrders`.
# MAGIC    - Use uma instrução `INSERT INTO FactOrders` para carregar os dados.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Inserir dados na tabela FactOrders
# MAGIC INSERT INTO FactOrders
# MAGIC (
# MAGIC   order_id,          -- Identificador exclusivo para o pedido
# MAGIC   dim_customer_key,  -- Foreign key referenciando a dimensão do cliente
# MAGIC   dim_date_key,      -- Foreign key fazendo referência à dimensão de data
# MAGIC   total_price,       -- Preço total do pedido
# MAGIC   order_status,      -- Estado do pedido
# MAGIC   order_priority,    -- Prioridade do pedido
# MAGIC   clerk,             -- Escriturário que cuida do pedido
# MAGIC   ship_priority,     -- Prioridade de envio do pedido
# MAGIC   comment            -- Comentários adicionais sobre o pedido
# MAGIC )
# MAGIC SELECT
# MAGIC   ro.order_id,       -- Selecione order_id de refined_orders
# MAGIC   dc.dim_customer_key, -- Selecione dim_customer_key de DimCustomer
# MAGIC   dd.dim_date_key,   -- Selecione dim_date_key de DimDate
# MAGIC   ro.total_price,    -- Selecione total_price de refined_orders
# MAGIC   ro.order_status,   -- Selecione order_status de refined_orders
# MAGIC   ro.order_priority, -- Selecione order_priority de refined_orders
# MAGIC   ro.clerk,          -- Selecione o funcionário de refined_orders
# MAGIC   ro.ship_priority,  -- Selecione ship_priority de refined_orders
# MAGIC   ro.comment         -- Selecione o comentário de refined_orders
# MAGIC FROM IDENTIFIER(:silver_schema || '.' || 'refined_orders') ro
# MAGIC JOIN DimCustomer dc
# MAGIC   ON ro.customer_id = dc.customer_id
# MAGIC   AND dc.is_current = TRUE -- Realize um join em customer_id e certifique-se de que o registro do cliente esteja atual.
# MAGIC JOIN DimDate dd
# MAGIC   ON ro.order_date = dd.full_date -- Realize um join em order_date e full_date.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validação
# MAGIC
# MAGIC **Tarefa:** Validar as contagens de registros em cada tabela **gold**.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Verifique a contagem de registros na tabela `DimCustomer`.
# MAGIC    - Use `SELECT COUNT(*) FROM DimCustomer` para verificar o número de registros.
# MAGIC
# MAGIC 2. Verifique a contagem de registros na tabela `DimDate`.
# MAGIC    - Use `SELECT COUNT(*) FROM DimDate` para verificar o número de registros.
# MAGIC
# MAGIC 3. Verifique a contagem de registros na tabela `FactOrders`.
# MAGIC    - Use `SELECT COUNT(*) FROM FactOrders` para verificar o número de registros.
# MAGIC
# MAGIC Verifique se o número esperado de registros está presente em cada tabela após a carga inicial.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros para a tabela DimCustomer
# MAGIC SELECT 'DimCustomer' AS table_name, COUNT(*) AS record_count FROM DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros para a tabela DimDate
# MAGIC SELECT 'DimDate' AS table_name, COUNT(*) AS record_count FROM DimDate

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros para a tabela FactOrders
# MAGIC SELECT 'FactOrders' AS table_name, COUNT(*) AS record_count FROM FactOrders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 12: Atualizações incrementais (SCD Tipo 2 MERGE)
# MAGIC
# MAGIC **Tarefa:** Executar atualizações incrementais em `DimCustomer` para alterações nos dados do cliente.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Detectar alterações em `refined_customer` (por exemplo, endereço alterado, novo cliente).
# MAGIC 2. Use uma instrução **MERGE** para fechar o registro antigo e inserir um novo registro em uma única operação.
# MAGIC    - Atualize o registro existente em `DimCustomer` definindo `end_date = CURRENT_DATE()` e `is_current = FALSE`.
# MAGIC    - Insira um novo registro com atributos atualizados: `start_date = CURRENT_DATE()`, `end_date = NULL` e `is_current = TRUE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.1 Exemplo: Criar alterações incrementais fictícias
# MAGIC
# MAGIC **Tarefa:** Simular alterações incrementais para dados do cliente.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Crie uma alteração fictícia para um cliente existente (por exemplo, `customer_id = 101`) alterando seu endereço.
# MAGIC 2. Crie uma entrada fictícia para um novo cliente (por exemplo, `customer_id = 99999`).

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar uma exibição temporária simulando atualizações incrementais, incluindo um cliente existente com informações atualizadas e um novo cliente com detalhes iniciais
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW incremental_customer_updates AS
# MAGIC ---- Cliente existente com informações atualizadas
# MAGIC SELECT 101    AS customer_id,
# MAGIC        'CHANGED Name'   AS name,
# MAGIC        'Updated Address 500' AS address,
# MAGIC        77     AS nation_key,
# MAGIC        '555-NEW-8888'   AS phone,
# MAGIC        CAST(999.99 AS DECIMAL(12,2)) AS acct_bal,
# MAGIC        'NEW_SEGMENT'    AS market_segment,
# MAGIC        'Existing row changed' AS comment
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC ---- Novo cliente com informações iniciais
# MAGIC SELECT 99999,
# MAGIC        'Completely New',
# MAGIC        '123 New Street',
# MAGIC        99,
# MAGIC        '999-999-1234',
# MAGIC        CAST(500.00 AS DECIMAL(12,2)),
# MAGIC        'MARKET_NEW',
# MAGIC        'Newly added customer';

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir o conteúdo da exibição temporária para verificar os dados
# MAGIC SELECT * FROM incremental_customer_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.2 MERGE único para SCD Tipo 2
# MAGIC
# MAGIC **Tarefa:** Executar uma instrução **MERGE** para lidar com atualizações e inserções de novos clientes.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Crie uma instrução `MERGE` que:
# MAGIC    - Identifica o registro antigo (caso o cliente já exista) e o marca como fechado (`is_current = FALSE`, `end_date = CURRENT_DATE()`).
# MAGIC    - Insere uma nova linha para qualquer cliente alterado (com atributos atualizados, `is_current = TRUE`, `start_date = CURRENT_DATE()`, e `end_date = NULL`).
# MAGIC    - Para novos clientes, insira apenas a nova linha com `is_current = TRUE`, `start_date = CURRENT_DATE()` e `end_date = NULL`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explicação:
# MAGIC
# MAGIC **Tarefa:** Entenda como a instrução **MERGE** funciona.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Se o registro antigo for encontrado, atualize-o para fechar o registro atual definindo `is_current = FALSE` e `end_date = CURRENT_DATE()`.
# MAGIC 2. Para qualquer cliente novo ou alterado, insira um novo registro com `is_current = TRUE`, um novo `start_date` e não `end_date`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Executar uma atualização incremental usando uma instrução MERGE para atualizar registros existentes e inserir novos registros na tabela DimCustomer com base em alterações nos dados do cliente
# MAGIC WITH staged_changes AS (
# MAGIC   ---- linha "OLD": Usado para localizar e atualizar o registro de dimensão ativa existente
# MAGIC   SELECT
# MAGIC     i.customer_id,
# MAGIC     i.name,
# MAGIC     i.address,
# MAGIC     i.nation_key,
# MAGIC     i.phone,
# MAGIC     i.acct_bal,
# MAGIC     i.market_segment,
# MAGIC     i.comment,
# MAGIC     'OLD' AS row_type
# MAGIC   FROM incremental_customer_updates i
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   ---- linha "NEW": Usado para inserir um registro de dimensão totalmente novo
# MAGIC   SELECT
# MAGIC     i.customer_id,
# MAGIC     i.name,
# MAGIC     i.address,
# MAGIC     i.nation_key,
# MAGIC     i.phone,
# MAGIC     i.acct_bal,
# MAGIC     i.market_segment,
# MAGIC     i.comment,
# MAGIC     'NEW' AS row_type
# MAGIC   FROM incremental_customer_updates i
# MAGIC )
# MAGIC
# MAGIC ---- Executar a operação de mesclagem na tabela DimCustomer
# MAGIC MERGE INTO DimCustomer t
# MAGIC USING staged_changes s
# MAGIC   ON t.customer_id = s.customer_id
# MAGIC      AND t.is_current = TRUE
# MAGIC      AND s.row_type = 'OLD'
# MAGIC
# MAGIC ---- Quando uma correspondência for encontrada, atualize o registro existente para fechá-lo
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     t.is_current = FALSE,
# MAGIC     t.end_date   = CURRENT_DATE()
# MAGIC
# MAGIC ---- Quando nenhuma correspondência for encontrada, insira o novo registro como a versão atual
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     customer_id,
# MAGIC     name,
# MAGIC     address,
# MAGIC     nation_key,
# MAGIC     phone,
# MAGIC     acct_bal,
# MAGIC     market_segment,
# MAGIC     comment,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     is_current
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     s.customer_id,
# MAGIC     s.name,
# MAGIC     s.address,
# MAGIC     s.nation_key,
# MAGIC     s.phone,
# MAGIC     s.acct_bal,
# MAGIC     s.market_segment,
# MAGIC     s.comment,
# MAGIC     CURRENT_DATE(),
# MAGIC     NULL,
# MAGIC     TRUE
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.3 Validar as linhas atualizadas
# MAGIC
# MAGIC **Tarefa:** Validar as atualizações dos dados do cliente.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Query e exiba os detalhes do cliente existente com `customer_id = 101`.
# MAGIC    - Use `SELECT * FROM DimCustomer WHERE customer_id = 101` para visualizar as versões antigas e novas.
# MAGIC 2. Query e exiba os detalhes do novo cliente com `customer_id = 99999`.
# MAGIC    - Use `SELECT * FROM DimCustomer WHERE customer_id = 99999` para visualizar o registro recém-inserido.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query para exibir detalhes de um cliente existente com o customer_id 101
# MAGIC SELECT 
# MAGIC   dim_customer_key,  -- Key exclusiva para o cliente na tabela de dimensões
# MAGIC   customer_id,       -- ID do cliente
# MAGIC   name,              -- Nome do cliente
# MAGIC   address,           -- Endereço do cliente
# MAGIC   is_current,        -- Sinalizador indicando se o registro é atual
# MAGIC   start_date,        -- Data de início do registro
# MAGIC   end_date           -- Data de término do registro
# MAGIC FROM 
# MAGIC   DimCustomer        -- Tabela de dimensões contendo dados do cliente
# MAGIC WHERE 
# MAGIC   customer_id = 101  -- Condição do filtro para selecionar o cliente com ID 101
# MAGIC ORDER BY 
# MAGIC   dim_customer_key   -- Ordene os resultados pela key exclusiva do cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query para exibir detalhes de um novo cliente com o ID do cliente 99999
# MAGIC SELECT 
# MAGIC   dim_customer_key,  -- Key exclusiva para o cliente na tabela de dimensões
# MAGIC   customer_id,       -- ID do cliente
# MAGIC   name,              -- Nome do cliente
# MAGIC   address,           -- Endereço do cliente
# MAGIC   is_current,        -- Sinalizador indicando se o registro é atual
# MAGIC   start_date,        -- Data de início do registro
# MAGIC   end_date           -- Data de término do registro
# MAGIC FROM 
# MAGIC   DimCustomer        -- Tabela de dimensões contendo dados do cliente
# MAGIC WHERE 
# MAGIC   customer_id = 99999 -- Condição do filtro para selecionar o cliente com ID 99999
# MAGIC ORDER BY 
# MAGIC   dim_customer_key   -- Ordene os resultados pela key exclusiva do cliente

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado esperado:**  
# MAGIC - Para `customer_id = 101`, o registro antigo terá `is_current = FALSE`, e uma nova versão será inserida com `is_current = TRUE`.
# MAGIC - Para `customer_id = 99999`, só existirá um registro, com `is_current = TRUE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 13: Queries de exemplo na Star Schema
# MAGIC
# MAGIC **Tarefa:** Execute algumas queries de exemplo para analisar os dados nas tabelas `FactOrders`, `DimCustomer` e `DimDate`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13.1 Contagens de linhas
# MAGIC
# MAGIC **Tarefa:** Exibir a contagem de registros nas tabelas `DimCustomer`, `DimDate` e `FactOrders`.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Query para exibir a contagem de registros na tabela `DimCustomer`.
# MAGIC    - Use `SELECT COUNT(*) FROM DimCustomer`.
# MAGIC
# MAGIC 2. Query para exibir a contagem de registros na tabela `DimDate`.
# MAGIC    - Use `SELECT COUNT(*) FROM DimDate`.
# MAGIC
# MAGIC 3. Query para exibir a contagem de registros na tabela `FactOrders`.
# MAGIC    - Use `SELECT COUNT(*) FROM FactOrders`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela DimCustomer com o nome da tabela
# MAGIC SELECT 'DimCustomer' AS table_name, COUNT(*) AS record_count FROM DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela DimDate com o nome da tabela
# MAGIC SELECT 'DimDate' AS table_name, COUNT(*) AS record_count FROM DimDate

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela FactOrders com o nome da tabela
# MAGIC SELECT 'FactOrders' AS table_name, COUNT(*) AS record_count FROM FactOrders

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13.2 Exemplo de query: Principais Segmentos de Mercado
# MAGIC
# MAGIC **Tarefa:** Exibir o valor total gasto pelos clientes em cada segmento de mercado, limitado aos 10 principais segmentos.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Query para calcular o valor total gasto pelos clientes em cada segmento de mercado.
# MAGIC    - Use `SELECT market_segment, SUM(order_amount) AS total_spent FROM FactOrders f JOIN DimCustomer dc ON f.customer_id = dc.customer_id GROUP BY market_segment ORDER BY total_spent DESC LIMIT 10`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir o valor total gasto pelos clientes em cada segmento de mercado, limitado aos 10 principais segmentos
# MAGIC SELECT 
# MAGIC   dc.market_segment,                -- Selecione o segmento de mercado na tabela de dimensão DimCustomer
# MAGIC   SUM(f.total_price) AS total_spent -- Calcule o valor total gasto somando o total_price da tabela de fatos FactOrders
# MAGIC FROM 
# MAGIC   FactOrders f                      -- Tabela de fatos contendo dados do pedido
# MAGIC JOIN 
# MAGIC   DimCustomer dc                    -- Tabela de dimensões contendo dados do cliente
# MAGIC   ON f.dim_customer_key = dc.dim_customer_key -- Condição de join na key do cliente
# MAGIC GROUP BY 
# MAGIC   dc.market_segment                 -- Agrupar os resultados por segmento de mercado
# MAGIC ORDER BY 
# MAGIC   total_spent DESC                  -- Ordene os resultados pelo valor total gasto em ordem decrescente
# MAGIC LIMIT 10                            -- Limitar os resultados aos 10 principais segmentos de mercado

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13.3 Exemplo de query: Contagens de pedidos por ano
# MAGIC
# MAGIC **Tarefa:** Conte o número de pedidos para cada ano unindo a tabela `FactOrders` com a tabela `DimDate`.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Query para contar o número de pedidos para cada ano.
# MAGIC    - Use `SELECT dd.year, COUNT(*) AS orders_count FROM FactOrders f JOIN DimDate dd ON f.dim_date_key = dd.dim_date_key GROUP BY dd.year ORDER BY dd.year`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Conte o número de pedidos para cada ano unindo a tabela FactOrders com a tabela DimDate
# MAGIC SELECT 
# MAGIC   dd.year,            -- Selecione o ano na tabela de dimensão DimDate
# MAGIC   COUNT(*) AS orders_count -- Conte o número de pedidos para cada ano
# MAGIC FROM 
# MAGIC   FactOrders f        -- Tabela de fatos contendo dados do pedido
# MAGIC JOIN 
# MAGIC   DimDate dd          -- Tabela de dimensão contendo dados de data
# MAGIC   ON f.dim_date_key = dd.dim_date_key -- Condição de join na key de data
# MAGIC GROUP BY 
# MAGIC   dd.year             -- Agrupar os resultados por ano
# MAGIC ORDER BY 
# MAGIC   dd.year             -- Ordenar os resultados por ano

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 14: Criando tabelas do Data Vault 2.0
# MAGIC
# MAGIC **Tarefa:** Criar componentes principais do Data Vault 2.0 — Hubs, Links e Satélites para modelar entidades de negócios e seus relacionamentos.

# COMMAND ----------

# MAGIC %md
# MAGIC Comece definindo o esquema prata como o esquema default no DBSQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Alternar para o esquema prata usando o comando USE SCHEMA SQL
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.1 Criar tabelas de Hub
# MAGIC
# MAGIC **Tarefa:** Criar tabelas de Hub para armazenar keys de negócios e metadados exclusivos.
# MAGIC
# MAGIC Passos a executar:
# MAGIC
# MAGIC 1. Crie a tabela `HubCustomer`.
# MAGIC    - Esta tabela armazena keys de negócios exclusivas do cliente.
# MAGIC
# MAGIC 2. Crie a tabela `HubOrder`.
# MAGIC    - Esta tabela armazena keys de negócios exclusivas de pedidos.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela Hub para o Cliente
# MAGIC CREATE TABLE IF NOT EXISTS H_Customer
# MAGIC (
# MAGIC   customer_hk STRING NOT NULL COMMENT 'MD5(customer_id)',
# MAGIC   customer_id INT NOT NULL,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_h_customer PRIMARY KEY (customer_hk)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela Hub para pedidos
# MAGIC CREATE TABLE IF NOT EXISTS H_Order
# MAGIC (
# MAGIC   order_hk STRING NOT NULL COMMENT 'MD5(order_id)',
# MAGIC   order_id INT NOT NULL,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_h_order PRIMARY KEY (order_hk)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.2 Criar tabelas de link
# MAGIC
# MAGIC **Tarefa:** Criar tabelas de link para representar relacionamentos entre entidades de negócios.
# MAGIC
# MAGIC Passos a executar:
# MAGIC
# MAGIC 1. Crie a tabela de link `L_Customer_Order`.
# MAGIC    - Esta tabela mapeia os clientes para seus pedidos usando keys compostas com hash.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Cria uma tabela de links para mapear clientes para seus pedidos com uma key primária com hash
# MAGIC CREATE TABLE IF NOT EXISTS L_Customer_Order
# MAGIC (
# MAGIC   customer_order_hk STRING NOT NULL COMMENT 'MD5(customer_hk||order_hk)',
# MAGIC   customer_hk STRING NOT NULL,
# MAGIC   order_hk STRING NOT NULL,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_l_customer_order PRIMARY KEY (customer_order_hk)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.3 Criar tabela satélite
# MAGIC
# MAGIC **Tarefa 1:** Criar tabelas satélite para armazenar atributos descritivos e rastrear alterações históricas.
# MAGIC
# MAGIC Passos a executar:
# MAGIC
# MAGIC 1. Crie a tabela `Sat_Customer_Info`.
# MAGIC    - Esta tabela contém informações descritivas sobre os clientes.
# MAGIC
# MAGIC 2. Crie a tabela `Sat_Order_Info`.
# MAGIC    - Esta tabela contém informações descritivas sobre pedidos.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar o satélite para armazenar as informações descritivas do cliente
# MAGIC CREATE TABLE IF NOT EXISTS S_Customer
# MAGIC (
# MAGIC   customer_hk STRING NOT NULL,
# MAGIC   hash_diff STRING NOT NULL COMMENT 'MD5 of all descriptive columns',
# MAGIC   name STRING,
# MAGIC   address STRING,
# MAGIC   nation_key INT,
# MAGIC   phone STRING,
# MAGIC   acct_bal DECIMAL(12,2),
# MAGIC   market_segment STRING,
# MAGIC   comment STRING,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_s_customer PRIMARY KEY (customer_hk, load_timestamp)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela satélite para armazenar as Informações Descritivas do Pedido
# MAGIC CREATE TABLE IF NOT EXISTS S_Order
# MAGIC (
# MAGIC   order_hk STRING NOT NULL,
# MAGIC   hash_diff STRING NOT NULL COMMENT 'MD5 of all descriptive columns',
# MAGIC   order_status STRING,
# MAGIC   total_price DECIMAL(12,2),
# MAGIC   order_date DATE,
# MAGIC   order_priority STRING,
# MAGIC   clerk STRING,
# MAGIC   ship_priority INT,
# MAGIC   comment STRING,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_s_order PRIMARY KEY (order_hk, load_timestamp)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Tarefa 2:** Defina o catálogo e o esquema default para operações do Spark SQL.
# MAGIC
# MAGIC Passos a executar:
# MAGIC
# MAGIC 1. Defina o catálogo default para o catálogo principal.
# MAGIC 2. Defina o esquema default como esquema prata.
# MAGIC

# COMMAND ----------

# Defina o catálogo default para seu catálogo principal e o esquema para o esquema prata no spark sql
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {silver_schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o catálogo atual para o nome do catálogo extraído
# MAGIC USE CATALOG IDENTIFIER(:catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o esquema atual para o nome do esquema definido
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 15: Processo ETL
# MAGIC
# MAGIC **Tarefa:** Carregue dados refinados nas tabelas do Data Vault 2.0 seguindo uma abordagem ETL estruturada.
# MAGIC
# MAGIC As subetapas incluem:
# MAGIC 1. Carregando Hubs  
# MAGIC 2. Carregando Links  
# MAGIC 3. Carregando Satélites
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.1 Definir funções auxiliares
# MAGIC
# MAGIC **Tarefa:** Definir funções Python para gerar hash keys e colunas de hash diff para clientes, pedidos e seus relacionamentos.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Defina uma função para gerar hash keys do cliente.
# MAGIC 2. Defina uma função para gerar hash keys de pedido.
# MAGIC 3. Defina uma função para gerar hash keys de link de pedido do cliente.
# MAGIC 4. Defina uma função para gerar uma coluna hash_diff para controle de alterações.
# MAGIC

# COMMAND ----------

# Gerar keys de hash e colunas de hash diff para entidades do Data Vault, incluindo cliente, pedido e seus relacionamentos, usando MD5
from pyspark.sql.functions import md5, concat_ws, col

# Defina uma função personalizada para gerar uma key de hash para customer_id
def generate_customer_hash_keys(df):
    return df.withColumn(
        "customer_hk", 
        md5(col("customer_id").cast("string"))
    )

# Defina uma função personalizada para gerar uma key hash para order_id
def generate_order_hash_keys(df):
    return df.withColumn(
        "order_hk", 
        md5(col("order_id").cast("string"))
    )

# Definir uma função personalizada para gerar uma key hash composta para cliente e pedido
def generate_customer_order_hash_key(df):
    return df.withColumn(
        "customer_order_hk",
        md5(concat_ws("||", col("customer_hk"), col("order_hk")))
    )

# Definir uma função personalizada para gerar uma diferença de hash para detecção de alterações
def generate_hash_diff(df, columns):
    return df.withColumn("hash_diff", md5(concat_ws("||", *[col(c) for c in columns])))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.2 Carregar tabelas refinadas para a camada prata
# MAGIC
# MAGIC **Tarefa 1:** Crie tabelas de dimensões e fatos refinadas renomeando colunas e convertendo tipos de dados.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Crie a tabela de dimensão do cliente refinada na camada prata.
# MAGIC 2. Crie a tabela de fatos de pedidos refinados na camada prata.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Cria uma tabela de dimensões do cliente refinada na camada prata
# MAGIC CREATE TABLE IF NOT EXISTS silver.refined_customer (
# MAGIC   customer_id INT NOT NULL,
# MAGIC   name STRING,
# MAGIC   address STRING,
# MAGIC   nation_key INT,
# MAGIC   phone STRING,
# MAGIC   acct_bal DECIMAL(12, 2),
# MAGIC   market_segment STRING,
# MAGIC   comment STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Cria uma tabela de fatos de pedidos refinados na camada prata
# MAGIC CREATE TABLE IF NOT EXISTS silver.refined_orders (
# MAGIC   order_id INT NOT NULL,
# MAGIC   customer_id INT NOT NULL,
# MAGIC   order_status STRING,
# MAGIC   total_price DECIMAL(12, 2),
# MAGIC   order_date DATE,
# MAGIC   order_priority STRING,
# MAGIC   clerk STRING,
# MAGIC   ship_priority INT,
# MAGIC   comment STRING
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Tarefa 2:** Definir e executar funções de carga ETL.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Defina funções de carga ETL para clientes e pedidos.
# MAGIC 2. Execute as funções de carregamento ETL para preparar dados para tabelas do Data Vault.

# COMMAND ----------

# Definindo funções de carga ETL
from pyspark.sql.functions import col, to_date, current_timestamp

def etl_refined_customer():
    bronze_customer = spark.table("bronze.customer")
    refined_customer = bronze_customer.select(
        col("c_custkey").cast("int").alias("customer_id"),
        col("c_name").alias("name"),
        col("c_address").alias("address"),
        col("c_nationkey").cast("int").alias("nation_key"),
        col("c_phone").alias("phone"),
        col("c_acctbal").cast("decimal(12,2)").alias("acct_bal"),
        col("c_mktsegment").alias("market_segment"),
        col("c_comment").alias("comment")
    )
    refined_customer.write.mode("overwrite").saveAsTable("silver.refined_customer")

def etl_refined_orders():
    bronze_orders = spark.table("bronze.orders")
    refined_orders = bronze_orders.select(
        col("o_orderkey").cast("int").alias("order_id"),
        col("o_custkey").cast("int").alias("customer_id"),
        col("o_orderstatus").alias("order_status"),
        col("o_totalprice").cast("decimal(12,2)").alias("total_price"),
        to_date(col("o_orderdate"), "yyyy-MM-dd").alias("order_date"),
        col("o_orderpriority").alias("order_priority"),
        col("o_clerk").alias("clerk"),
        col("o_shippriority").cast("int").alias("ship_priority"),
        col("o_comment").alias("comment")
    )
    refined_orders.write.mode("overwrite").saveAsTable("silver.refined_orders")

# COMMAND ----------

# Executar carga ETL
etl_refined_customer()
etl_refined_orders()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.3 Carregar hub do cliente
# MAGIC
# MAGIC **Tarefa:** Carregue e mescle dados do cliente na tabela do hub H_Customer.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Aplique a geração de hash key aos dados refinados do cliente.
# MAGIC 2. Mescle novos registros de clientes com metadados na tabela de hub.
# MAGIC 3. Visualize os primeiros 10 registros no hub H_Customer.

# COMMAND ----------

# Carregue e mescle novos registros de clientes na tabela do hub H_Customer com key de hash e metadados
from pyspark.sql.functions import current_timestamp, lit
silver_customer_df = spark.sql("SELECT * FROM silver.refined_customer")

customer_hub_data = (
    generate_customer_hash_keys(silver_customer_df)
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("TPC-H"))
)

customer_hub_data.createOrReplaceTempView("customer_hub_stage")

spark.sql("""
MERGE INTO H_Customer AS target
USING customer_hub_stage AS source
ON target.customer_hk = source.customer_hk
WHEN NOT MATCHED THEN
  INSERT (customer_hk, customer_id, load_timestamp, record_source)
  VALUES (source.customer_hk, source.customer_id, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Visualizar os primeiros 10 registros da tabela de hub do H_Customer
# MAGIC SELECT * FROM H_Customer LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.4 Carregar Satélite do Cliente
# MAGIC
# MAGIC **Tarefa:** Carregar e mesclar dados descritivos do cliente na tabela satélite S_Customer.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Gere hash_diff a partir de atributos descritivos do cliente.
# MAGIC 2. Mesclar registros na tabela satélite com metadados.

# COMMAND ----------

# Mesclar novos registros descritivos de clientes na tabela satélite S_Customer com hash diff e metadados
customer_sat_columns = ["name", "address", "nation_key", "phone", "acct_bal", "market_segment", "comment"]

customer_sat_data = generate_hash_diff(customer_hub_data, customer_sat_columns)
customer_sat_data.createOrReplaceTempView("customer_sat_stage")

spark.sql(f"""
MERGE INTO S_Customer AS target
USING customer_sat_stage AS source
ON target.customer_hk = source.customer_hk AND target.load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN
  INSERT (customer_hk, hash_diff, {', '.join(customer_sat_columns)}, load_timestamp, record_source)
  VALUES (source.customer_hk, source.hash_diff, {', '.join([f'source.{col}' for col in customer_sat_columns])}, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.5 Hub de Ordem de Carga e Satélite
# MAGIC
# MAGIC **Tarefa:** Carregue e mescle dados de pedidos nas tabelas satélites H_Order e S_Order.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Gere hash keys para pedidos e hash_diff para campos descritivos.
# MAGIC 2. Mescle registros de pedidos nas tabelas hub e satélite com metadados.

# COMMAND ----------

# Carregue e mescle novos registros de pedidos em tabelas de hub e satélite com chaves hash, hash diff e metadados
silver_orders_df = spark.sql("SELECT * FROM silver.refined_orders")

order_hub_data = (
    generate_order_hash_keys(silver_orders_df)
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("TPC-H"))
)

order_hub_data.createOrReplaceTempView("order_hub_stage")

spark.sql("""
MERGE INTO H_Order AS target
USING order_hub_stage AS source
ON target.order_hk = source.order_hk
WHEN NOT MATCHED THEN
  INSERT (order_hk, order_id, load_timestamp, record_source)
  VALUES (source.order_hk, source.order_id, source.load_timestamp, source.record_source)
""")

order_sat_columns = ["order_status", "total_price", "order_date", "order_priority", "clerk", "ship_priority", "comment"]

order_sat_data = generate_hash_diff(order_hub_data, order_sat_columns)
order_sat_data.createOrReplaceTempView("order_sat_stage")

spark.sql(f"""
MERGE INTO S_Order AS target
USING order_sat_stage AS source
ON target.order_hk = source.order_hk AND target.load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN
  INSERT (order_hk, hash_diff, {', '.join(order_sat_columns)}, load_timestamp, record_source)
  VALUES (source.order_hk, source.hash_diff, {', '.join([f'source.{col}' for col in order_sat_columns])}, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.6 Carregar link pedido do cliente
# MAGIC
# MAGIC **Tarefa:** Carregue e mescle dados de relacionamento pedido do cliente na tabela de link L_Customer_Order.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Gere uma hash key composta a partir de hash key do cliente e do pedido.
# MAGIC 2. Mesclar registros de link na tabela de links com metadados.

# COMMAND ----------

# Criar e mesclar registros de link pedido do cliente com key hash e metadados combinados na tabela de links
from pyspark.sql.functions import concat_ws

link_data = (
    # Junte silver_orders_df a H_Customer e H_Order usando as key naturais
    silver_orders_df.alias("orders")
    .join(spark.table("H_Customer").alias("hc"), on=[col("orders.customer_id") == col("hc.customer_id")], how="inner")
    .join(spark.table("H_Order").alias("ho"), on=[col("orders.order_id") == col("ho.order_id")], how="inner")
    # Selecione as keys de hash já estabelecidas para ambos os hubs
    .select(
        col("hc.customer_hk").alias("customer_hk"),
        col("ho.order_hk").alias("order_hk")
    )
    # Criar uma key hash combinada
    .withColumn("customer_order_hk", md5(concat_ws("||", col("customer_hk"), col("order_hk"))))
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("TPC-H"))
)

link_data.createOrReplaceTempView("link_stage")


spark.sql("""
MERGE INTO L_Customer_Order AS target
USING link_stage AS source
ON target.customer_order_hk = source.customer_order_hk
WHEN NOT MATCHED THEN
  INSERT (customer_order_hk, customer_hk, order_hk, load_timestamp, record_source)
  VALUES (source.customer_order_hk, source.customer_hk, source.order_hk, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 16: Criando views de negócios
# MAGIC
# MAGIC **Tarefa:** Crie visualizações de negócios que unam Hubs, Links e Satélites para simplificar as queries para usuários finais.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Junte-se ao hub do cliente, ao satélite do cliente, ao hub de encomendas e ao satélite de encomendas utilizando a tabela Link.
# MAGIC 2. Crie uma view de negócios combinando detalhes do cliente e do pedido.
# MAGIC    - Esta view pode ser posteriormente materializada na camada Ouro.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Crie uma View de Negócios combinando detalhes do Cliente e do Pedido (Isso pode ser materializado na camada Ouro, mas para este laboratório é apresentado como uma view)
# MAGIC
# MAGIC CREATE OR REPLACE VIEW gold.BV_Customer_Order AS
# MAGIC SELECT 
# MAGIC     hc.customer_id,
# MAGIC     sc.name AS customer_name,
# MAGIC     sc.address AS customer_address,
# MAGIC     ho.order_id,
# MAGIC     so.order_date,
# MAGIC     so.total_price,
# MAGIC     so.order_status
# MAGIC FROM 
# MAGIC     H_Customer hc
# MAGIC JOIN 
# MAGIC     S_Customer sc ON hc.customer_hk = sc.customer_hk
# MAGIC JOIN 
# MAGIC     L_Customer_Order lco ON hc.customer_hk = lco.customer_hk
# MAGIC JOIN 
# MAGIC     H_Order ho ON lco.order_hk = ho.order_hk
# MAGIC JOIN 
# MAGIC     S_Order so ON ho.order_hk = so.order_hk;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 17: Exemplo de query
# MAGIC
# MAGIC **Tarefa:** Execute uma query de exemplo para demonstrar como usar a view de negócios criada a partir do modelo do Data Vault.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Query o conteúdo completo da view de negócios criada na etapa anterior.
# MAGIC 2. Calcula o total de vendas por cliente e apresenta resultados em ordem decrescente.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query o conteúdo completo da view de negócios criada na etapa anterior
# MAGIC select * from gold.BV_Customer_Order

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Calcule o total de vendas por cliente e exiba os resultados em ordem decrescente
# MAGIC SELECT 
# MAGIC     customer_name,
# MAGIC     SUM(total_price) AS total_sales
# MAGIC FROM 
# MAGIC     gold.BV_Customer_Order
# MAGIC GROUP BY 
# MAGIC     customer_name
# MAGIC ORDER BY 
# MAGIC     total_sales DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 18: Etapas de verificação
# MAGIC
# MAGIC **Tarefa:** Execute a verificação básica para garantir a integridade e a correção do modelo do Data Vault.
# MAGIC
# MAGIC Passos a executar:
# MAGIC
# MAGIC 1. Verifique o número de registros em cada tabela do Data Vault (Hubs, Links e Satélites).
# MAGIC 2. Verifique se cada pedido está associado a exatamente um cliente.
# MAGIC    - Contar pedidos com um e vários clientes associados para garantir a integridade da ligação.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verificar contagens de registros
# MAGIC SELECT 'H_Customer' AS table_name, COUNT(*) AS record_count FROM H_Customer
# MAGIC UNION ALL
# MAGIC SELECT 'H_Order' AS table_name, COUNT(*) AS record_count FROM H_Order
# MAGIC UNION ALL
# MAGIC SELECT 'L_Customer_Order' AS table_name, COUNT(*) AS record_count FROM L_Customer_Order
# MAGIC UNION ALL
# MAGIC SELECT 'S_Customer' AS table_name, COUNT(*) AS record_count FROM S_Customer
# MAGIC UNION ALL
# MAGIC SELECT 'S_Order' AS table_name, COUNT(*) AS record_count FROM S_Order;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verifique se cada pedido está associado a exatamente um cliente
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_orders,
# MAGIC   SUM(CASE WHEN customer_count = 1 THEN 1 ELSE 0 END) AS orders_with_one_customer,
# MAGIC   SUM(CASE WHEN customer_count != 1 THEN 1 ELSE 0 END) AS orders_with_multiple_customers
# MAGIC FROM (
# MAGIC   SELECT order_hk, COUNT(DISTINCT customer_hk) AS customer_count
# MAGIC   FROM L_Customer_Order
# MAGIC   GROUP BY order_hk
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 19: Criando ou atualizando recursos
# MAGIC
# MAGIC **Tarefa:** Crie recursos no nível do cliente usando as tabelas `refined_orders` e `refined_customer`.
# MAGIC
# MAGIC Recursos a serem criadas:
# MAGIC - `total_orders`: Número total de pedidos por cliente  
# MAGIC - `avg_order_value`: Preço médio por pedido  
# MAGIC - `total_spending`: Valor total gasto no geral  
# MAGIC - `market_segment`: segmento de mercado do cliente

# COMMAND ----------

# Gere recursos básicos no nível do cliente agregando dados do pedido e juntando com as informações do cliente
from pyspark.sql.functions import col, count, avg, sum, current_timestamp

orders_df = spark.sql("SELECT * FROM refined_orders")
customers_df = spark.sql("SELECT * FROM refined_customer")

base_features_df = (
    orders_df.groupBy("customer_id")
    .agg(
        count("*").alias("total_orders"),
        avg("total_price").alias("avg_order_value"),
        sum("total_price").alias("total_spending")
    )
    .join(
        customers_df.select("customer_id", "market_segment"),
        on="customer_id",
        how="inner"
    )
    .withColumn("feature_update_ts", current_timestamp())
)

display(base_features_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 19.1: Registrar ou mesclar-atualizar tabela de recursos
# MAGIC
# MAGIC **Tarefa:** Crie uma tabela de recursos nomeada `customer_features` na esquema ouro. Se a tabela já existir, mescle os novos dados.

# COMMAND ----------

# Criar ou mesclar-atualizar a tabela 'customer_features' com recursos agregados no nível do cliente
feature_table_name = f"{catalog_name}.{gold_schema}.customer_features"

try:
    fs.create_table(
        name=feature_table_name,
        primary_keys=["customer_id"],
        schema=base_features_df.schema,
        description="Customer-level features derived from refined tables."
    )
    print(f"Feature table '{feature_table_name}' created.")
except Exception as e:
    print(f"Feature table might already exist: {e}")

fs.write_table(
    name=feature_table_name,
    df=base_features_df,
    mode="merge"  
)

print(f"Feature table '{feature_table_name}' updated with new features.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 19.2: Atualizações de agendamento (Opcional)
# MAGIC
# MAGIC **Tarefa:** Crie um subconjunto deste notebook e agende-o como um Databricks Job para atualizar periodicamente os recursos com dados atualizados.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 20: Treinamento de modelos com o Repositório de recursos
# MAGIC
# MAGIC **Tarefa:** Treine um modelo de classificação binário usando recursos armazenadas no repositório de recursos e dados refinados do cliente.
# MAGIC
# MAGIC Execute o seguinte:
# MAGIC 1. Crie um rótulo simples: Os clientes que gastaram mais do que um determinado limite são considerados "grandes gastadores."
# MAGIC 2. Procure os mesmos recursos via `FeatureLookup` para treinamento.
# MAGIC 3. Treine um modelo básico de Regressão Logística.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 20.1 Criar rótulo binário para classificação
# MAGIC
# MAGIC **Tarefa:** Adicione um rótulo binário ao dataset do cliente onde os gastadores altos (total_spending > limite) são rotulados como 1, outros como 0.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Use as funções `when` e `otherwise` do PySpark para definir uma coluna de rótulo.
# MAGIC 2. Selecione colunas de recursos e rótulos relevantes para o treinamento do modelo.

# COMMAND ----------

# Adicionar rótulo binário aos clientes com base em se seus gastos totais excedem um limite
from pyspark.sql.functions import when

threshold = 20000.0

labeled_df = (
    base_features_df
    .withColumn("label", when(col("total_spending") > threshold, 1).otherwise(0))
    .select("customer_id", "total_orders", "avg_order_value", "total_spending", "market_segment", "label")
)

display(labeled_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 20.2 Crie um conjunto de treinamento com FeatureLookups
# MAGIC
# MAGIC **Tarefa:** Use o repositório de recursos para procurar recursos pré-computados e associá-los aos dados do cliente rotulados.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Defina um objeto com nomes de recursos `FeatureLookup` e join keys necessários.
# MAGIC 2. Remova colunas duplicadas do DataFrame rotulado para evitar conflitos.
# MAGIC 3. Crie um conjunto de treinamento usando `fs.create_training_set()` e carregue-o como um DataFrame do Spark.

# COMMAND ----------

# Crie um conjunto de treinamento pesquisando recursos do repositório de recursos e juntando-os com dados rotulados
from databricks.feature_store import FeatureLookup

feature_lookup = FeatureLookup(
    table_name=feature_table_name,
    feature_names=[
        "total_orders",
        "avg_order_value",
        "total_spending",
        "market_segment",
    ],
    lookup_key="customer_id",
)
labeled_df_clean = labeled_df.drop("total_orders", "avg_order_value", "total_spending", "market_segment")

training_set = fs.create_training_set(
    df=labeled_df_clean,
    feature_lookups=[feature_lookup],
    label="label",
    exclude_columns=["feature_update_ts"]
)

training_df = training_set.load_df()
display(training_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 20.3 Treinar um Modelo de Regressão Logística Simples
# MAGIC
# MAGIC **Tarefa:** Treine um modelo de regressão logística simples usando um pipeline que inclui transformações e classificação de recursos.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Codifique o índice e a coluna categórica one-hot `market_segment`.
# MAGIC 2. Use `VectorAssembler` para combinar recursos numéricos e codificados em um único vetor.
# MAGIC 3. Defina um modelo `LogisticRegression`.
# MAGIC 4. Crie um `Pipeline` com os estágios de transformação e ajuste o modelo no dataset de treinamento.

# COMMAND ----------

# Construir e treinar um modelo de regressão logística usando um pipeline com etapas de transformação de recursos
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

# Indexador de strings para a coluna categórica
indexer = StringIndexer(inputCol="market_segment", outputCol="market_segment_idx", handleInvalid="keep")
encoder = OneHotEncoder(inputCols=["market_segment_idx"], outputCols=["market_segment_vec"])

assembler = VectorAssembler(
    inputCols=["total_orders", "avg_order_value", "total_spending", "market_segment_vec"],
    outputCol="features"
)

lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])
model = pipeline.fit(training_df)

print("Model training complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 20.4 Registrar modelo no MLflow (opcional)
# MAGIC
# MAGIC **Tarefa:** (Opcional) Registre o modelo treinado no MLflow para controle de versão, rastreamento e implantação.
# MAGIC
# MAGIC **Observação:** Esta etapa é opcional e não está implementada neste bloco de anotações de laboratório.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 21: Inferência em lote usando o repositório de recursos
# MAGIC
# MAGIC **Cenário**: Temos alguns IDs de clientes novos ou existentes e queremos prever quais podem ser os que gastam muito. Vamos fazer:
# MAGIC 1. Demonstrar a criação de um DataFrame de IDs de clientes.
# MAGIC 2. Procure as mesmos recursos via `FeatureLookup`.
# MAGIC 3. Gere previsões com nosso pipeline treinado.
# MAGIC
# MAGIC **Tarefa:** Prever clientes que gastam alto usando seus dados de recursos e um modelo de ML treinado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 21.1 Crie lista de clientes de amostra
# MAGIC
# MAGIC **Tarefa:** Selecione um pequeno conjunto de IDs de cliente para inferência em lote.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Selecione alguns IDs de cliente (por exemplo, 5 linhas) no DataFrame de treinamento.
# MAGIC 2. Adicione uma coluna de sinalizador chamada `batch_inference_example` para indicar que esse subconjunto é para fins de demonstração.

# COMMAND ----------

# Selecione 5 clientes de amostra e adicione uma coluna de sinalizador para demonstração de inferência em lote
from pyspark.sql.functions import lit

sample_customers = (
    training_df.select("customer_id")
    .limit(5)
    .withColumn("batch_inference_example", lit(True))
)

display(sample_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 21.2 Recuperar recursos e pontuar
# MAGIC
# MAGIC **Tarefa:** Pesquise recursos de repositório de recursos para os clientes de exemplo e gere previsões usando o modelo treinado.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Use `FeatureLookup` para recuperar os recursos necessários da tabela de recursos com base em `customer_id`.
# MAGIC 2. Crie um conjunto de inferências combinando clientes de amostra com os recursos pesquisados.
# MAGIC 3. Carregue o DataFrame de inferência e aplique o pipeline de modelo treinado.
# MAGIC 4. Exiba os rótulos e probabilidades previstos para cada cliente.

# COMMAND ----------

# Realizar inferência em lotes recuperando recursos para clientes de amostra e aplicando o modelo treinado
inference_lookup = FeatureLookup(
    table_name=feature_table_name,
    feature_names=[
        "total_orders",
        "avg_order_value",
        "total_spending",
        "market_segment"
    ],
    lookup_key="customer_id"
)

inference_set = fs.create_training_set(
    df=sample_customers,
    feature_lookups=[inference_lookup],
    label=None 
)

inference_df = inference_set.load_df()
predictions = model.transform(inference_df)
display(predictions.select("customer_id", "prediction", "probability"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza
# MAGIC Agora, limpe seu ambiente de trabalho.
# MAGIC
# MAGIC Execute o script abaixo para excluir o catálogo `catalog_name` e todos os seus objetos, se ele existir.

# COMMAND ----------

# Exclua o catálogo junto com todos os objetos (esquemas, tabelas) no dentro.
spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC Remova todos os widgets criados durante a demonstração para limpar o ambiente do notebook.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC Neste laboratório, você praticou uma ampla variedade de técnicas modernas de data warehousing e engenharia de recursos de ML no Databricks. Você modelou relacionamentos usando ERM, manipulou dados históricos usando SCD Tipo 2 e aplicou a estrutura do Data Vault 2.0 para criar modelos de dados escalonáveis e flexíveis. Você também criou e gerenciou recursos com o repositório de recursos de Databricks, treinou modelos de ML e realizou inferência em lote. Juntos, esses exercícios forneceram experiência prática no projeto de pipelines integrados de dados e ML para casos de uso analíticos do mundo real.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
