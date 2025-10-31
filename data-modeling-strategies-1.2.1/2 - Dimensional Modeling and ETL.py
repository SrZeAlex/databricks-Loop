# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Modelagem Dimensional e ETL
# MAGIC Nesta demonstração, exploraremos como implementar Slowly Changing Dimensions (SCD) Tipo 2 usando Databricks em uma star schema. Você trabalhará no processo de criação de tabelas de dimensões e fatos, aplicação de transformações de dados e manutenção de registros históricos usando SCD Tipo 2. Este exercício prático fortalecerá suas habilidades de modelagem de dados e ETL usando o Spark SQL e o Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final desta demonstração, você será capaz de:
# MAGIC - Aplicar conceitos de SCD Tipo 2 para rastrear mudanças históricas em tabelas de dimensões usando Databricks.
# MAGIC - Projetar uma star schema para querying e análise eficientes em um ambiente de data warehouse.
# MAGIC - Construir pipelines de ETL usando Spark SQL e Delta Lake para transformar e carregar dados.
# MAGIC - Avaliar a precisão e completude dos dados utilizando técnicas de validação de dados.
# MAGIC - Desenvolver fluxos de trabalho de dados escaláveis e automatizados usando Databricks notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tarefas a Executar
# MAGIC
# MAGIC Neste notebook, exploraremos como construir um *modelo dimensional* (especificamente, uma star schema) a partir de nossos dados TPC-H, usando tabelas **silver** (refinadas) e **gold** (dimensões e fatos).
# MAGIC
# MAGIC Ao fazer isso, percorreremos as seguintes etapas:
# MAGIC
# MAGIC 1. **Define Our Table Structures**: Entenda como `GENERATED ALWAYS AS IDENTITY` funciona e crie todas as tabelas necessárias (prata e ouro).  
# MAGIC 2. **Load the Silver Layer**: Mova dados de tabelas `bronze` TPC-H (brutas) para as tabelas refinadas, aplicando as transformações necessárias (renomeando colunas, cortando strings, normalizando tipos de dados).  
# MAGIC 3. **Create an SCD Type 2 Dimension**: Explore como criar uma Dimensão de Mudança Lenta que preserve o histórico de registros alterados.  
# MAGIC 4. **Load the Gold Layer**: Preencha as tabelas de dimensões e fatos com dados **initial** e **incremental**. Demonstrar como usar instrução única `MERGE` para atualizações SCD Tipo 2.  
# MAGIC 5. **Validate and Explore Sample Queries**: Verifique nossas cargas de dados e query nosso novo esquema de estrelas para obter percepções.  
# MAGIC
# MAGIC No final, teremos um esquema de estrela **gold** com uma dimensão **SCD Tipo 2** (`DimCustomer`) que rastreia as mudanças históricas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pré-requisitos
# MAGIC Como lembrete, você já deve ter executado um **script de configuração** no notebook anterior que criou:  
# MAGIC    - Um catálogo específico do usuário.  
# MAGIC    - Esquemas: `bronze`, `silver` e `gold`.  
# MAGIC    - Tabelas TPC-H copiadas para `bronze`.
# MAGIC
# MAGIC Se, por qualquer motivo, você ainda não tiver realizado essa configuração, retorne agora para **1 - Modelagem de Entidades e Relacionamentos e Manipulação de Restrições** e execute o comando '%run' na célula 8 desse notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨OBRIGATÓRIO - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Antes de executar células neste notebook, selecione seu cluster de compute clássico no laboratório. Lembre-se de que **Serverless** está habilitado por default.
# MAGIC
# MAGIC Siga estas etapas para selecionar o cluster de compute clássico:
# MAGIC * Navegue até o canto superior direito deste notebook e clique no menu dropdown para selecionar seu cluster. Por default, o notebook usará **Serverless**. <br>
# MAGIC
# MAGIC ##### **📌**Se o cluster estiver disponível, selecione-o e continue para a próxima célula. Se o cluster não for mostrado:
# MAGIC   - Na lista suspensa, selecione **More**.
# MAGIC   - No pop-up **Attach to an existing compute resource**, selecione a primeira lista dropdown. Você verá um nome de cluster exclusivo nessa lista dropdown. Selecione esse cluster.
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

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1: Definições de tabela
# MAGIC
# MAGIC Para explorar a modelagem dimensional, definiremos:  
# MAGIC - Tabelas **Prata** (às vezes chamadas de camada *refined* ou *integration*).  
# MAGIC - Tabelas **Gold** que implementam um **star schema** com uma dimensão SCD Tipo 2.  
# MAGIC
# MAGIC Um recurso importante que usaremos no Delta `GENERATED ALWAYS AS IDENTITY`, que atribui automaticamente valores numéricos incrementais para chaves substitutas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Defina seu catálogo

# COMMAND ----------

# DBTITLE 1,USE CATALOG
import re

# Obtenha o e-mail do usuário atual e extraia o nome do catálogo dividindo em '@' e tomando a primeira parte
user_id = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]

# Substitua todos os caracteres especiais no `user_id` por um sublinhado '_' para criar o nome do catálogo
catalog_name = re.sub(r'[^a-zA-Z0-9]', '_', user_id)

# COMMAND ----------

# Defina o nome do esquema para as camadas prata e ouro
silver_schema = "silver"
gold_schema = "gold"

# COMMAND ----------

# Criar um widget para capturar o nome do catálogo e todos os nomes de esquema
dbutils.widgets.text("catalog_name", catalog_name)
dbutils.widgets.text("silver_schema", silver_schema)
dbutils.widgets.text("gold_schema", gold_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o catálogo atual como o nome do catálogo extraído no DBSQL
# MAGIC USE CATALOG IDENTIFIER(:catalog_name);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Criar tabelas Pratas
# MAGIC
# MAGIC O esquema **silver** normalmente é um local para dados refinados. Definiremos dois exemplos de tabelas refinadas:  
# MAGIC - `refined_customer` (baseado em TPC-H `customer`)  
# MAGIC - `refined_orders` (baseado em TPC-H `orders`)
# MAGIC
# MAGIC Cada tabela renomeará e padronizará colunas. Nós só definimos o esquema aqui; Carregaremos os dados em seguida.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o esquema atual para o nome do silver_schema extraído no DBSQL
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema);

# COMMAND ----------

# DBTITLE 1,CREATE TABLE IF NOT EXISTS refined_customer
# MAGIC %sql
# MAGIC -- Criando a tabela refined_customer se ela ainda não existir
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

# DBTITLE 1,CREATE TABLE IF NOT EXISTS refined_orders
# MAGIC %sql
# MAGIC -- Criando a tabela refined_orders se ela ainda não existir
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
# MAGIC ### 1.3 Criar tabelas Ouros (Star Schema)
# MAGIC
# MAGIC #### Definimos:
# MAGIC - **`DimCustomer`** (com atributos SCD Tipo 2)  
# MAGIC - **`DimDate`**  
# MAGIC - **`FactOrders`**  
# MAGIC
# MAGIC #### Principais Passos
# MAGIC 1. `GENERATED ALWAYS AS IDENTITY` para keys substitutas.  
# MAGIC 2. Colunas adicionais para `DimCustomer` gerenciar o SCD Tipo 2 (por exemplo, `start_date`, `end_date`, e `is_current`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o esquema atual como o nome do gold_schema extraído no DBSQL
# MAGIC USE SCHEMA IDENTIFIER(:gold_schema);

# COMMAND ----------

# DBTITLE 1,CREATE TABLE IF NOT EXISTS DimCustomer
# MAGIC %sql
# MAGIC -- Crie a tabela DimCustomer para armazenar detalhes do cliente com atributos SCD (Slowly Changing Dimension) Tipo 2 para rastreamento histórico
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
# MAGIC   CONSTRAINT pk_dim_customer PRIMARY KEY (dim_customer_key)  -- Restrição de Primary key na chave substituta
# MAGIC );

# COMMAND ----------

# DBTITLE 1,CREATE TABLE IF NOT EXISTS DimDate
# MAGIC %sql
# MAGIC -- Tabela DimDate simples para armazenar informações relacionadas à data
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
# MAGIC -- Verifique o catálogo atual
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verifique o esquema atual
# MAGIC SELECT current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC **Nota:** Substitua os valores `<default_catalog_name>` e `<default_schema_name>` em ambas as linhas no código abaixo pelos nomes de catálogo e esquema default reais da saída da query nas células anteriores:
# MAGIC
# MAGIC **Linhas de código de destino:**
# MAGIC ```dbsql
# MAGIC CONSTRAINT fk_customer FOREIGN KEY (dim_customer_key) REFERENCES <default_catalog_name>.<default_schema_name>.DimCustomer(dim_customer_key),  -- Foreign key constraint linking to DimCustomer
# MAGIC
# MAGIC CONSTRAINT fk_date FOREIGN KEY (dim_date_key) REFERENCES <default_catalog_name>.<default_schema_name>.DimDate(dim_date_key)  -- Foreign key constraint linking to DimDate
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,CREATE TABLE IF NOT EXISTS FactOrders
# MAGIC %sql
# MAGIC -- Criação da tabela FactOrders fazendo referência a DimCustomer e DimDate
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
# MAGIC   CONSTRAINT fk_customer FOREIGN KEY (dim_customer_key) REFERENCES <default_catalog_name>.<default_schema_name>.DimCustomer(dim_customer_key),  -- Restrição de Foreign key vinculando ao DimCustomer
# MAGIC   CONSTRAINT fk_date FOREIGN KEY (dim_date_key) REFERENCES <default_catalog_name>.<default_schema_name>.DimDate(dim_date_key)  -- Restrição de Foreign key vinculando a DimDate
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notas sobre `GENERATED ALWAYS AS IDENTITY`
# MAGIC - Cada tabela gera automaticamente números exclusivos para a coluna da key substituta.  
# MAGIC - Você não insere um valor para essas colunas; A Delta lida com isso perfeitamente.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2: Carregando dados na Prata
# MAGIC
# MAGIC Carregaremos dados das tabelas TPC-H `bronze` em `refined_customer` e `refined_orders`. Esta etapa pressupõe que seu dataset TPC-H esteja em `bronze.customer` e `bronze.orders`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alterne para o catálogo usando o nome do catálogo extraído no DBSQL
# MAGIC USE CATALOG IDENTIFIER(:catalog_name);
# MAGIC
# MAGIC -- Alterne para o esquema prata no DBSQL
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema);

# COMMAND ----------

# DBTITLE 1,INSERT INTO silver.refined_customer FROM bronze.customer
# MAGIC %sql
# MAGIC -- Insira dados transformados da tabela bronze.customer na tabela refined_customer
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

# DBTITLE 1,INSERT INTO silver.refined_orders FROM bronze.orders
# MAGIC %sql
# MAGIC -- Inserir dados transformados da tabela bronze.orders na tabela refined_orders
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
# MAGIC #### Validação
# MAGIC Verifique se os registros foram carregados em `refined_customer` e `refined_orders`:

# COMMAND ----------

# DBTITLE 1,display(refined_customer), display(refined_orders)
# MAGIC %sql
# MAGIC -- Exibe a contagem de registros na tabela refined_customer
# MAGIC SELECT COUNT(*) AS refined_customer_count FROM refined_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibe a contagem de registros na tabela refined_orders
# MAGIC SELECT COUNT(*) AS refined_orders_count FROM refined_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibe a contagem de registros na tabela refined_customer
# MAGIC SELECT COUNT(*) AS refined_customer_count FROM refined_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibe a contagem de registros na tabela refined_orders
# MAGIC SELECT COUNT(*) AS refined_orders_count FROM refined_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3: Carga Inicial em Ouro (Modelo Dimensional)
# MAGIC
# MAGIC #### Principais Passos:
# MAGIC 1. Execute uma **initial load** de `DimCustomer` (todos os clientes como *current*).  
# MAGIC 2. Crie entradas de data em `DimDate` de `refined_orders`. (Esta pode ser uma tabela pré-carregada para datas diárias por vários anos)  
# MAGIC 3. Preencha `FactOrders`, vinculando cada ordem às keys de dimensão corretas.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alterne para o esquema dourado usando o comando USE SCHEMA SQL
# MAGIC USE SCHEMA IDENTIFIER(:gold_schema);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Carga inicial do DimCustomer (SCD Tipo 2)
# MAGIC - Marque cada linha com `start_date = CURRENT_DATE()`, `end_date = NULL`, e `is_current = TRUE`.

# COMMAND ----------

# DBTITLE 1,INSERT INTO DimCustomer de silver.refined_customer
# MAGIC %sql
# MAGIC -- Inserir dados na tabela de dimensões DimCustomer
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
# MAGIC FROM IDENTIFIER(:silver_schema || '.' || 'refined_customer')   -- Tabela de origem na esquema prata

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 DimDate
# MAGIC - Coletar valores exclusivos `order_date` de `refined_orders`.  
# MAGIC - Use funções embutidas para dividi-las em `day, month, year`.

# COMMAND ----------

# DBTITLE 1,INSERT INTO DimDate FROM silver.refined_orders
# MAGIC %sql
# MAGIC -- Inserir datas distintas na tabela de dimensão DimDate
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
# MAGIC ### 3.3 FactOrders
# MAGIC - Vincular cada pedido a `DimCustomer` e `DimDate`.  
# MAGIC - Fazemos join em `(customer_id = dc.customer_id AND is_current = TRUE)` para SCD Tipo 2, garantindo que correspondemos apenas a um registro de dimensão ativa.

# COMMAND ----------

# DBTITLE 1,INSERT INTO FactOrders FROM silver.refined_orders JOIN DimCustomer JOIN DimDate
# MAGIC %sql
# MAGIC -- Inserir dados na tabela FactOrders
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
# MAGIC Verifique as contagens de registros em cada tabela **gold**:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir a contagem de registros para a tabela DimCustomer
# MAGIC SELECT 'DimCustomer' AS table_name, COUNT(*) AS record_count FROM DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir a contagem de registros para a tabela DimDate
# MAGIC SELECT 'DimDate' AS table_name, COUNT(*) AS record_count FROM DimDate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir a contagem de registros para a tabela FactOrders
# MAGIC SELECT 'FactOrders' AS table_name, COUNT(*) AS record_count FROM FactOrders

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 4: Atualizações incrementais (SCD Tipo 2 MERGE)
# MAGIC
# MAGIC Na vida real, você detectaria mudanças em `refined_customer` ao longo do tempo (por exemplo, um endereço alterado ou um novo cliente). Quando você vê uma diferença:
# MAGIC 1. **Close** o registro antigo em DimCustomer ('end_date = <current date>`, `is_current = FALSE').  
# MAGIC 2. **Insert** um novo registro (com os atributos atualizados, 'start_date = <current date>`, `end_date = NULL`, `is_current = TRUE').  
# MAGIC
# MAGIC Abaixo está uma única instrução MERGE que pode atualizar o registro antigo e inserir a nova versão em uma única passagem.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Exemplo: Criar alterações incrementais fictícias
# MAGIC
# MAGIC Isso simula:  
# MAGIC - Um cliente *existente* (ID=101) alterando seu endereço.  
# MAGIC - Um cliente *novinho em folha* (ID=99999).

# COMMAND ----------

# DBTITLE 1,CREATE OR REPLACE TEMP VIEW incremental_customer_updates
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW incremental_customer_updates AS
# MAGIC -- Cliente existente com informações atualizadas
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
# MAGIC -- Novo cliente com informações iniciais
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
# MAGIC -- Exibir o conteúdo da exibição temporária para verificar os dados
# MAGIC SELECT * FROM incremental_customer_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 MERGE único para SCD Tipo 2
# MAGIC 1. Produzimos duas linhas para *qualquer cliente alterado*: um rotulado `"OLD"` para fechar o registro atual e outro rotulado `"NEW"` para inserir uma versão atualizada.  
# MAGIC 2. Para um cliente verdadeiramente novo, produzimos apenas uma linha `"NEW"`.

# COMMAND ----------

# DBTITLE 1,spark.sql(merge_sql)
# MAGIC %sql
# MAGIC WITH staged_changes AS (
# MAGIC   -- Linha "OLD": Usado para localizar e atualizar o registro de dimensão ativa existente
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
# MAGIC   -- Linha "NEW": Usado para inserir um registro de dimensão totalmente novo
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
# MAGIC -- Executar a operação de mesclagem na tabela DimCustomer
# MAGIC MERGE INTO DimCustomer t
# MAGIC USING staged_changes s
# MAGIC   ON t.customer_id = s.customer_id
# MAGIC      AND t.is_current = TRUE
# MAGIC      AND s.row_type = 'OLD'
# MAGIC
# MAGIC -- Quando uma correspondência for encontrada, atualize o registro existente para fechá-lo
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     t.is_current = FALSE,
# MAGIC     t.end_date   = CURRENT_DATE()
# MAGIC
# MAGIC -- Quando nenhuma correspondência for encontrada, insira o novo registro como a versão atual
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
# MAGIC #### Explicação:
# MAGIC - Se for encontrada uma linha correspondente ao registro `"OLD"` (o que significa que o cliente está ativo no `DimCustomer` neste momento), nós **atualizaremos** esse registro para fechá-lo (`is_current=FALSE` e `end_date=TODAY`).  
# MAGIC - Cada linha `"NEW"` não corresponde, por isso aciona o caminho "INSERT". Inserimos uma nova linha com `is_current=TRUE`, uma nova `start_date` e sem `end_date`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Validar as linhas atualizadas

# COMMAND ----------

# DBTITLE 1,SELECT FROM DimCustomer
# MAGIC %sql
# MAGIC -- Query para exibir detalhes de um cliente existente com o customer_id 101
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
# MAGIC -- Query para exibir detalhes de um novo cliente com o customer_id 99999
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
# MAGIC Você deve ver que a versão antiga do `customer_id = 101` agora tem `is_current=FALSE`, e uma nova versão foi inserida. Um novo `customer_id=99999` tem apenas um registro (`is_current=TRUE`).

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 5: Queries de exemplo na Star Schema
# MAGIC
# MAGIC Agora que temos `FactOrders` vinculando-se a `DimCustomer` e `DimDate`, vamos executar algumas verificações para ver como podemos analisar os dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Contagens de linhas

# COMMAND ----------

# DBTITLE 1,display(DimCustomer), display(DimState), display(FactOrders)
# MAGIC %sql
# MAGIC -- Exibe a contagem de registros na tabela DimCustomer com o nome da tabela
# MAGIC SELECT 'DimCustomer' AS table_name, COUNT(*) AS record_count FROM DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibe a contagem de registros na tabela DimDate com o nome da tabela
# MAGIC SELECT 'DimDate' AS table_name, COUNT(*) AS record_count FROM DimDate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibe a contagem de registros na tabela FactOrders com o nome da tabela
# MAGIC SELECT 'FactOrders' AS table_name, COUNT(*) AS record_count FROM FactOrders

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Exemplo de query: Principais Segmentos de Mercado

# COMMAND ----------

# DBTITLE 1,SELECT FROM FactOrders JOIN DimCustomer
# MAGIC %sql
# MAGIC -- Exibe o valor total gasto pelos clientes em cada segmento de mercado, limitado aos 10 principais segmentos
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
# MAGIC ### 5.3 Exemplo de query: Contagens de pedidos por ano

# COMMAND ----------

# DBTITLE 1,SELECT FROM FactOrders JOIN DimDate
# MAGIC %sql
# MAGIC -- Conte o número de pedidos para cada ano unindo a tabela FactOrders com a tabela DimDate
# MAGIC SELECT 
# MAGIC   dd.year,            -- Selecione o ano na tabela de dimensão DimDate
# MAGIC   COUNT(*) AS orders_count -- Conte o número de pedidos para cada ano
# MAGIC FROM 
# MAGIC   FactOrders f        -- Tabela de fatos contendo dados do pedido
# MAGIC JOIN 
# MAGIC   DimDate dd          -- Tabela de dimensão contendo dados de data
# MAGIC   ON f.dim_date_key = dd.dim_date_key -- Condição de joion na key de data
# MAGIC GROUP BY 
# MAGIC   dd.year             -- Agrupar os resultados por ano
# MAGIC ORDER BY 
# MAGIC   dd.year             -- Ordenar os resultados por ano

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 6: Resumo & Próximas etapas
# MAGIC
# MAGIC 1. **Estruturas de tabela definidas**:  
# MAGIC    - Tabelas **Silver** (refinadas/integração) (`refined_customer`, `refined_orders`).  
# MAGIC    - Tabelas **Gold** de dimensão/fato (`DimCustomer`, `DimDate`, `FactOrders`).  
# MAGIC    - Usado `GENERATED ALWAYS AS IDENTITY` para keys substitutas.  
# MAGIC    - Gerenciei SCD Tipo 2 no `DimCustomer` usando `start_date`, `end_date` e `is_current`.  
# MAGIC
# MAGIC 2. **Dados iniciais carregados**:  
# MAGIC    - Movido de `bronze` TPC-H para **silver**.  
# MAGIC    - Tabelas de dimensões e fatos **gold** preenchidas.  
# MAGIC
# MAGIC 3. **Atualizações incrementais**:  
# MAGIC    - Demonstrou um único MERGE para SCD Tipo 2.  
# MAGIC    - Fechei registros de dimensões antigas e inseri novos simultaneamente.  
# MAGIC
# MAGIC 4. **Validação**:  
# MAGIC    - Mostrou contagens de linhas, queries para confirmar a funcionalidade do star schema.  
# MAGIC
# MAGIC **Para onde ir a seguir**:  
# MAGIC - Incorporar verificações automatizadas de alterações `refined_customer` para manter `DimCustomer` sincronizado ao longo do tempo.  
# MAGIC - Estenda a lógica do Tipo 2 para outras dimensões, como atributos baseados em peça, fornecedor ou data.  
# MAGIC - Crie tabelas de fatos adicionais (por exemplo, FactLineItems) para aumentar seu star schema.  
# MAGIC - Explore queries avançadas de BI ou análise em seu star schema, aproveitando ao máximo as otimizações de desempenho do Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Você concluiu:
# MAGIC 1. Construindo e estruturando suas tabelas **silver**.  
# MAGIC 2. Projetando uma star schema **gold** com uma dimensão Tipo 2.  
# MAGIC 3. Carregando suas tabelas de dimensões e fatos com atualizações de dados **initial** e **incremental**.  
# MAGIC 4. Executando queries para validar e explorar a star schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza
# MAGIC
# MAGIC Antes de prosseguir, execute o bloco SQL na célula abaixo para limpar seu trabalho.

# COMMAND ----------

# DBTITLE 1,DROP tabelas de prata e ouro
# MAGIC %sql
# MAGIC -- Drop tabelas Pratas
# MAGIC DROP TABLE IF EXISTS silver.refined_customer;
# MAGIC DROP TABLE IF EXISTS silver.refined_orders;
# MAGIC
# MAGIC -- Drop tabelas Ouros
# MAGIC DROP TABLE IF EXISTS gold.DimCustomer;
# MAGIC DROP TABLE IF EXISTS gold.DimDate;
# MAGIC DROP TABLE IF EXISTS gold.FactOrders;

# COMMAND ----------

# MAGIC %md
# MAGIC Remova todos os widgets criados durante a demonstração para limpar o ambiente do notebook.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC Nesta demonstração, implementamos com sucesso um design de star schema para um data warehouse usando o Databricks. Aplicamos a metodologia SCD Tipo 2 para gerenciar dados históricos em tabelas de dimensões, garantindo a consistência e a rastreabilidade dos dados. Por meio da transformação de dados passo a passo e do desenvolvimento de pipeline de ETL, ganhamos experiência prática na criação e gerenciamento de modelos de dados para análise. Ao final desta demonstração, você terá desenvolvido proficiência na criação de pipelines de ETL escalonáveis e eficientes, na manutenção da linhagem de dados e na aplicação de conceitos de modelagem dimensional a datasets do mundo real.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
