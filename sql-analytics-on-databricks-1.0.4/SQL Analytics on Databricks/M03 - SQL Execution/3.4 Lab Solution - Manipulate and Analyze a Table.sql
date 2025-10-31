-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3.4 Laboratório - Manipular e analisar uma tabela
-- MAGIC
-- MAGIC ### Duração: ~15-20 minutos
-- MAGIC
-- MAGIC Neste laboratório, você manipulará uma tabela **camada de bronze** para criar uma tabela **camada de prata**. Em seguida, você criará uma **view materializada** programada para realizar uma análise simples na tabela e refresh-la diariamente.
-- MAGIC
-- MAGIC
-- MAGIC ### Objetivos
-- MAGIC - Aplicar técnicas básicas de transformação de dados para limpar e estruturar dados brutos para análise posterior
-- MAGIC - Combinar dados de várias fontes e realizar as operações necessárias para obter percepções significativas
-- MAGIC - Criar e gerenciar views que agregam dados e automatizam processos de refresh para análises aprimoradas e mais eficientes.

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

-- MAGIC %run ../Includes/3.4-Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a célula a seguir para definir e exibir seu catálogo e esquema default. Confirme se o catálogo default é **dbacademy** e se o esquema é **labuser** (isso usa a variável `DA.schema_name` criada no script de configuração da sala de aula).
-- MAGIC
-- MAGIC O catálogo e o esquema default são pré-configurados para você na célula abaixo. Isso elimina a necessidade de especificar o nome de três níveis para suas tabelas (ou seja, catalog.schema.table). Todas as tabelas serão lidas e gravadas no seu esquema **dbacademy.labuser**.

-- COMMAND ----------

-- Alterar o catálogo/esquema default
USE CATALOG dbacademy;
USE SCHEMA IDENTIFIER(DA.schema_name);


-- Ver catálogo e esquema atuais
SELECT 
  current_catalog(), 
  current_schema(), 
  DA.schema_name AS variable_value   -- Exibir valor da variável DA.schema_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Cenário de laboratório
-- MAGIC
-- MAGIC Você é analista da DB Inc., e recebeu a tarefa de trabalhar com dados do cliente armazenados em vários arquivos JSON. Os dados estão localizados no volume **dbacademy_retail.v01.retail-pipeline.customers.stream_json**. Seu objetivo é limpar, transformar e enriquecer os dados brutos e criar dois novos objetos:
-- MAGIC
-- MAGIC - **Tabela Prata**: Esta tabela conterá dados selecionados, limpos e ligeiramente transformados. Ele serve como um estágio intermediário entre os dados brutos e a tabela de nível ouro.
-- MAGIC
-- MAGIC - **View materializada de Ouro**: Essa view materializada conterá dados agregados que devem ser programados para serem refreshed todos os dias.
-- MAGIC
-- MAGIC Siga os passos detalhados abaixo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Ingestão e Exploração

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Execute a célula a seguir para criar uma tabela chamada **customers_lab_bronze** no esquema **dbacademy.labuser**. Os dados serão provenientes de uma série de arquivos JSON localizados no volume de dados compartilhados do Marketplace fornecido: `/Volumes/dbacademy_retail/v01/retail-pipeline/customers/stream_json/`
-- MAGIC

-- COMMAND ----------

-- Remova a tabela se já existir para começar do início
DROP TABLE IF EXISTS customers_lab_bronze;

-- Crie uma tabela vazia
CREATE TABLE customers_lab_bronze;

-- Copie os ficheiros JSON de um volume para a tabela e junte o esquema
COPY INTO customers_lab_bronze
FROM '/Volumes/dbacademy_retail/v01/retail-pipeline/customers/stream_json/'
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Execute a célula abaixo para fazer query da tabela **customers_lab_bronze** e visualizar 10 linhas. Observe o seguinte:
-- MAGIC
-- MAGIC    - A coluna **email** contém o domínio de e-mail do usuário.
-- MAGIC
-- MAGIC    - A coluna **estado** contém uma abreviação de estado dos EUA.
-- MAGIC    
-- MAGIC    - A coluna **carimbo de data/hora** contém um valor de carimbo de data/hora Unix.

-- COMMAND ----------

SELECT *
FROM customers_lab_bronze
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Escreva uma query abaixo para:
-- MAGIC
-- MAGIC    - Visualize os valores distintos na coluna **estado** da tabela **customers_lab_bronze** e nomeie a coluna **distinct_state**.
-- MAGIC    
-- MAGIC    - Ordene o resultado pela nova coluna **distinct_state** em ordem crescente.
-- MAGIC
-- MAGIC    Execute a query e visualize os resultados. Confirme se a query retorna 48 linhas e se o primeiro valor é `null`.

-- COMMAND ----------

SELECT distinct(state) AS distinct_state
FROM customers_lab_bronze
ORDER BY distinct_state;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Execute a query abaixo para query a tabela **state_lookup** e visualizar 10 linhas. Observe que a tabela contém nomes de estado abreviados, que podem ser usados para obter os nomes de estado completos.

-- COMMAND ----------

SELECT *
FROM state_lookup
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Manipulação e transformação de dados

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Crie a tabela **customer_lab_silver** preenchendo o seguinte:
-- MAGIC
-- MAGIC    a. Selecione as seguintes colunas da tabela **customers_lab_bronze**:
-- MAGIC       - **customer_id**
-- MAGIC       - **name**
-- MAGIC       - **address**
-- MAGIC       - **city**
-- MAGIC       - **Email**
-- MAGIC       - **operation**
-- MAGIC       - **zip_code**
-- MAGIC       - **state**
-- MAGIC    
-- MAGIC    b. Crie uma coluna chamada **sign_up_date** convertendo o carimbo de data/hora Unix (coluna **carimbo de data/hora**) em um formato de data legível por humanos usando o seguinte:
-- MAGIC       - [função from_unixtime](https://docs.databricks.com/aws/en/sql/language-manual/functions/from_unixtime)
-- MAGIC
-- MAGIC       - [função date_format](https://docs.databricks.com/aws/en/sql/language-manual/functions/date_format)
-- MAGIC
-- MAGIC       **DICA:**
-- MAGIC       ```sql
-- MAGIC       date_format(from_unixtime(timestamp), 'yyyy-MM-dd HH:mm:ss') AS sign_up_date
-- MAGIC       ```
-- MAGIC
-- MAGIC    c. Crie uma coluna chamada **email_domain** que extraia o domínio do endereço de e-mail usando a função `substring_index`.
-- MAGIC       - [função substring_index](https://docs.databricks.com/aws/en/sql/language-manual/functions/substring_index#:~:text=Returns%20the%20substring%20of%20expr,occurrence%20of%20the%20delimiter%20delim%20.)
-- MAGIC
-- MAGIC       - Exemplo de resultados: **peter@example.org** → **example.org**
-- MAGIC
-- MAGIC       **DICA:**
-- MAGIC       ```SQL`
-- MAGIC       substring_index(cust.email, '@', -1) AS email_domain,
-- MAGIC       ```
-- MAGIC
-- MAGIC    d. Execute um LEFT JOIN entre as tabelas **customers_lab_bronze** e **state_lookup** para reter todos os registros da tabela **customers_lab_bronze**, criando a coluna **state_name** da tabela **state_lookup** para obter o nome completo do estado, quando disponível.
-- MAGIC       - **DICA:** Tente usar o `generate with AI` em outra célula para ajudá-lo com a sintaxe de join, se precisar.
-- MAGIC
-- MAGIC **NOTA:** A tabela de solução já foi criada para você em seu esquema **labuser** se você quiser visualizá-la. A tabela é chamada **customer_lab_silver_solution**.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE customer_lab_silver AS
SELECT
  cust.customer_id,
  cust.name,
  cust.address,
  cust.city,
  cust.email,
  cust.operation,
  cust.zip_code,
  cust.state,
  date_format(from_unixtime(timestamp), 'yyyy-MM-dd HH:mm:ss') AS sign_up_date,
  substring_index(cust.email, '@', -1) AS email_domain,
  state.state_name
FROM customers_lab_bronze cust
  LEFT JOIN state_lookup state ON cust.state = state.state_abbreviation;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Execute a célula abaixo para comparar sua tabela **customer_lab_silver** com a tabela de solução **customer_lab_silver_solution**. Se as tabelas forem idênticas (nomes de colunas, todos os dados, etc.), a query deverá retornar 0 linhas. 
-- MAGIC
-- MAGIC Se a query retornar um erro, você terá que corrigir a tabela **customer_lab_silver** criada na célula anterior.

-- COMMAND ----------

SELECT *
FROM customer_lab_silver_solution
EXCEPT
SELECT
  customer_id,
  name,
  address,
  city,
  email,
  operation,
  zip_code,
  state,
  sign_up_date,
  email_domain,
  state_name
FROM customer_lab_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B3. Criar uma View Materializada

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Execute a célula abaixo para visualizar 10 linhas da tabela **customer_lab_silver**.

-- COMMAND ----------

SELECT *
FROM customer_lab_silver
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Crie uma view materializada chamada **customers_per_state_gold** que:
-- MAGIC
-- MAGIC    - Conta o número de clientes por **state_name** da tabela **customers_lab_silver** em uma coluna chamada **total_customers**.
-- MAGIC
-- MAGIC    - Garante que a view materializada se refresh todos os dias.
-- MAGIC    
-- MAGIC    - Ordena os resultados por **total_customers** em ordem decrescente.
-- MAGIC
-- MAGIC    [CREATE MATERIALIZED VIEW](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view)
-- MAGIC
-- MAGIC    **DICA:** Se você não conseguiu criar a tabela **customers_lab_silver** corretamente, você pode usar a tabela de solução pré-fabricada **customer_lab_silver_solution**.

-- COMMAND ----------

CREATE MATERIALIZED VIEW customers_per_state_gold 
SCHEDULE EVERY 1 DAY
AS
SELECT 
  state_name, 
  count(*) AS total_customers
FROM customer_lab_silver 
GROUP BY state_name
ORDER BY total_customers DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Execute a célula abaixo para visualizar sua view materializada. Confirme se as primeiras 10 linhas da view materializada correspondem aos seguintes valores:
-- MAGIC
-- MAGIC     | state_name | total_customers |
-- MAGIC     |-------------|-----------------|
-- MAGIC     | Califórnia | 259 |
-- MAGIC     | Texas | 141 |
-- MAGIC     | Nova Iorque | 98 |
-- MAGIC     | Flórida | 69 |
-- MAGIC     | Illinois | 55 |
-- MAGIC     | Michigan | 52 |
-- MAGIC     | Ohio | 50 |
-- MAGIC     | nulo | 49 |
-- MAGIC     | Washington | 38 |
-- MAGIC     | Arizona | 36 |
-- MAGIC

-- COMMAND ----------

SELECT *
FROM customers_per_state_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Escreva a instrução para descrever informações detalhadas sobre sua view materializada **customers_per_state_gold** para visualizar as informações refreshed. 
-- MAGIC
-- MAGIC    Confirme se a seção **# Informação de Refresh** é semelhante a este exemplo de solução:
-- MAGIC
-- MAGIC
-- MAGIC | **Informação de Refresh**     |  **Tipo de dado**                      |
-- MAGIC |-----------------------------|-----------------------------------|
-- MAGIC | **Última Refresh**           | 2025-03-20T20:45:05Z (pode ser diferente)             |
-- MAGIC | **Tipo de Última Refresh**        | RECOMPUTED                        |
-- MAGIC | **Estado de Última Refresh**    | Concluído com sucesso                         |
-- MAGIC | **Refresh mais recente**           | Sua atualização específica |
-- MAGIC | **Agendamento de Refresh**         | DIARIAMENTE                       |

-- COMMAND ----------

DESCRIBE EXTENDED customers_per_state_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
