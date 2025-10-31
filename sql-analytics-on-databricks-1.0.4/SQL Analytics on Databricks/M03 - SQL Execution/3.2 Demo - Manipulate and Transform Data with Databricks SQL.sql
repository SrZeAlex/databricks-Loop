-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3.2 Demonstração - Manipular e transformar dados com o Databricks SQL
-- MAGIC
-- MAGIC Nesta demonstração vamos explorar, manipular e analisar tabelas usando DBSQL.
-- MAGIC
-- MAGIC ### Objetivos
-- MAGIC - Realizar exploração de dados simples fazendo query das tabelas em DBSQL para entender a estrutura de dados, identificar colunas-chave e examinar as primeiras linhas.
-- MAGIC - Use funções DBSQL, instruções de caso e joins para realizar a transformação de dados e criar uma nova tabela contendo dados modificados com base em condições específicas.
-- MAGIC - Aplicar filtragem e estatísticas resumidas em DBSQL para extrair percepções significativas, usando funções como WHERE, GROUP BY e HAVING para análise de dados.
-- MAGIC - Realizar análise de dados usando common table expressions (CTEs), operações dinâmicas e funções de janelas para extrair percepções mais detalhados e realizar cálculos complexos.

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
-- MAGIC Lembre-se de que a configuração do laboratório é criada com o [0 - OBRIGATÓRIO - Configuração do Curso]($../0 - REQUIRED - Course Setup and Data Discovery). Se você encerrar a sessão de laboratório ou se a sessão atingir o limite de tempo, seu ambiente será redefinido e você precisará executar novamente a Configuração do curso do Notebook.

-- COMMAND ----------

-- MAGIC %run ../Includes/3.2-Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a célula a seguir para definir e exibir seu catálogo e esquema default. Confirme se o catálogo default é **dbacademy** e se o esquema é **labuser** (isso usa a variável `DA.schema_name` criada no script de configuração da sala de aula).
-- MAGIC
-- MAGIC O catálogo e o esquema default são pré-configurados para você na célula abaixo. Isso elimina a necessidade de especificar o nome de três níveis para suas tabelas (ou seja, catalog.schema.table). Todas as tabelas serão lidas e gravadas no seu esquema **dbacademy.labuser**.

-- COMMAND ----------

-- Alterar o catálogo/esquema default
USE CATALOG dbacademy;
USE SCHEMA IDENTIFIER(DA.schema_name);


-- Alterar o catálogo/esquema
SELECT 
  current_catalog(), 
  current_schema(), 
  DA.schema_name AS variable_value -- Exibir valor da variável DA.schema_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Exploração de Dados
-- MAGIC
-- MAGIC Com as tabelas já no catálogo **dbacademy** e dentro do esquema **labuser**, é hora de começar a explorar as tabelas usando DBSQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Vamos começar fazer query da tabela **aus_orders** para exibir uma visualização da tabela. Observe que esta tabela contém informações sobre pedidos de clientes na Austrália.

-- COMMAND ----------

SELECT *
FROM aus_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Você pode exibir as informações básicas de metadados de uma tabela com a instrução `DESCRIBE TABLE`. Execute a query. Observe que os resultados mostram o nome da coluna, o tipo de dados e se há um comentário na coluna.
-- MAGIC
-- MAGIC     [DESCRIBE TABLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table)

-- COMMAND ----------

DESCRIBE TABLE aus_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Você pode usar o comando `DESCRIBE TABLE EXTENDED` para exibir informações detalhadas sobre as colunas especificadas, incluindo as estatísticas de coluna coletadas pelo comando e informações adicionais de metadados da tabela.
-- MAGIC
-- MAGIC     Execute a célula e veja os resultados. Observe o seguinte:
-- MAGIC
-- MAGIC     - A partir da linha **9**, *Informações detalhadas da tabela* são mostradas abaixo.
-- MAGIC
-- MAGIC     - Na linha **Hora de Criação**, você pode ver quando esta tabela foi criada.
-- MAGIC
-- MAGIC     - Na linha **Tipo**, você pode ver se a tabela é gerenciada ou externa.
-- MAGIC
-- MAGIC     - Na fila **Proprietário**, você pode ver o dono da tabela.
-- MAGIC
-- MAGIC     - Uma variedade de outras informações detalhadas sobre a tabela é mostrada.
-- MAGIC

-- COMMAND ----------

DESCRIBE TABLE EXTENDED aus_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Visualizar os valores distintos na coluna **productid** e observe que há 7 valores **productid** exclusivos. Posteriormente, usaremos esta coluna para obter os nomes dos produtos da tabela **au_products_lookup**.

-- COMMAND ----------

SELECT DISTINCT(productid) AS distinct_productid
FROM aus_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Manipulação e transformação de dados
-- MAGIC
-- MAGIC Depois de explorar os dados, começaremos a transformá-los para análise posterior. Para o treinamento, vamos detalhar cada transformação passo a passo em cada célula. Para fins de teste, usaremos uma instrução `LIMIT` para verificar nossa lógica sem processar todo o dataset até que seja necessário.
-- MAGIC
-- MAGIC Nesta seção, concluiremos as seguintes transformações:
-- MAGIC
-- MAGIC - Renomeie colunas adicionando um sublinhado **_** entre os nomes das colunas.
-- MAGIC
-- MAGIC - Padronize os valores na coluna **salesrep** convertendo-os em maiúsculas.
-- MAGIC
-- MAGIC - Analise a coluna **orderdate** em componentes detalhados como ano, mês, dia, trimestre, etc.
-- MAGIC
-- MAGIC - Crie uma variável categórica baseada em **orderamt** para classificar o valor do pedido como *Alto*, *Médio* ou *Baixo*.
-- MAGIC
-- MAGIC - Join a tabela **aus_orders** com a tabela **au_products_lookup** para obter o **productname**.
-- MAGIC
-- MAGIC - Crie uma nova tabela chamada **aus_orders_silver**.
-- MAGIC
-- MAGIC Para referência, aqui está uma lista de [Funções integradas](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin)  que você pode usar para DBSQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Vamos visualizar todas as colunas e 10 linhas da tabela **aus_orders** usando a instrução `LIMIT`. Observe que esta tabela contém informações de pedidos para indivíduos na Austrália.

-- COMMAND ----------

SELECT *
FROM aus_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Vamos começar nosso processo reorganizando a ordem das colunas e renomeando-as adicionando um **_** entre as palavras nos nomes das colunas para facilitar a leitura. Execute a query e visualize os resultados. Observe que os nomes das colunas foram modificados.

-- COMMAND ----------

SELECT 
  orderid AS orders_id,
  customerid AS customer_id,
  productid AS product_id,
  quantity,
  orderamt AS order_amt,
  salesrep AS sales_rep,
  orderdate AS order_date
FROM aus_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Crie uma coluna chamada **order_country** com o valor *Australia* para indicar que todos esses pedidos são da Austrália. 
-- MAGIC
-- MAGIC     Execute a query e exiba a coluna **order_country**. Observe que a coluna contém o valor *Australia*.

-- COMMAND ----------

SELECT 
  orderid AS orders_id,
  customerid AS customer_id,
  productid AS product_id,
  quantity,
  orderamt AS order_amt,
  salesrep AS sales_rep,
  'Australia' AS order_country,        -- Crie a coluna order_country
  orderdate AS order_date
FROM aus_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Use a função `UPPER` para colocar em maiúsculas os valores na coluna **salesrep** para padronizar os valores nessa coluna caso haja inconsistências de maiúsculas e minúsculas.
-- MAGIC
-- MAGIC     Execute a query e exiba a coluna **salesrep**. Observe que todos os valores foram convertidos para maiúsculas.

-- COMMAND ----------

SELECT 
  orderid AS orders_id,
  customerid AS customer_id,
  productid AS product_id,
  quantity,
  orderamt AS order_amt,
  UPPER(salesrep) AS sales_rep,        -- Coloque os valores em maiúsculas
  'Australia' AS order_country, 
  orderdate AS order_date
FROM aus_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Crie uma coluna chamada **price_per_item** dividindo **order_amt** por **quantidade** e arredondando o resultado para duas casas decimais.
-- MAGIC
-- MAGIC     Execute a query e visualize os resultados. Observe que a coluna **price_per_item** foi calculada.

-- COMMAND ----------

-- DBTITLE 1,Calcular o preço por item
SELECT 
  orderid AS orders_id,
  customerid AS customer_id,
  productid AS product_id,
  quantity,
  orderamt AS order_amt,
  ROUND(order_amt/quantity, 2) AS price_per_item,          -- Calcular o price_per_item
  UPPER(salesrep) AS sales_rep,
  'Australia' AS order_country,
  orderdate AS order_date
FROM aus_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Para uma análise posterior detalhada, pode ser benéfico analisar a coluna **order_date** em valores de data específicos. Aqui, vamos analisar a coluna **order_date** em seis colunas para uma análise detalhada:
-- MAGIC
-- MAGIC    - **order_year** - [função ano](https://docs.databricks.com/aws/en/sql/language-manual/functions/year)
-- MAGIC
-- MAGIC    - **order_month** - [função mês](https://docs.databricks.com/aws/en/sql/language-manual/functions/month)
-- MAGIC
-- MAGIC    - **order_day** - [função dia](https://docs.databricks.com/aws/en/sql/language-manual/functions/day)
-- MAGIC
-- MAGIC    - **order_dayofweek** - [função nome do dia](https://docs.databricks.com/aws/en/sql/language-manual/functions/dayname)
-- MAGIC
-- MAGIC    - **order_quarter** - [função trimestre](https://docs.databricks.com/aws/en/sql/language-manual/functions/quarter)
-- MAGIC
-- MAGIC    - **order_year_quarter** - Concatenar o ano do pedido com o trimestre (por exemplo, 2020Q1) usando a [função concat](https://docs.databricks.com/aws/en/sql/language-manual/functions/concat)
-- MAGIC
-- MAGIC    - Você também pode usar a [função date_format](https://docs.databricks.com/aws/en/sql/language-manual/functions/date_format)  para conversões de data adicionais.
-- MAGIC
-- MAGIC    Execute a query e visualize os resultados. Observe todas as novas colunas **order_date**.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1, Calcular datas
SELECT 
  orderid AS orders_id,
  customerid AS customer_id,
  productid AS product_id,
  quantity,
  orderamt AS order_amt,
  ROUND(order_amt/quantity,2) AS price_per_item,
  UPPER(salesrep) AS sales_rep,
  'Australia' AS order_country,
  orderdate AS order_date,
  YEAR(order_date) AS order_year,                             -- Obtenha o ano
  MONTH(order_date) AS order_month,                           -- Obtenha o mês
  DAY(order_date) AS order_day,                               -- Obtenha o número do dia
  DAYNAME(order_date) AS order_dayofweek,                     -- Obtenha o dia da semana como uma string
  QUARTER(order_date) AS order_quarter,                       -- Obtenha o trimestre
  CONCAT(order_year,'-Q',order_quarter) AS order_year_quarter -- Crie o ano e o trimestre: 2020-Q1
FROM aus_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Vamos criar uma coluna chamada **order_amt_category** que especifica se o valor do pedido é *Baixo*, *Médio* ou *Alto* usando os seguintes valores:
-- MAGIC    - **Baixo**: Menor ou igual a 250.000
-- MAGIC    - **Médio**: Entre 250.000 e 1.000.000
-- MAGIC    - **Alto**: Maior ou igual a 1.000.000
-- MAGIC
-- MAGIC Para criar a coluna, use uma expressão `CASE WHEN` (expressão case). A `CASE WHEN` expressão usa uma expressão para testar uma condição. Se a condição for verdadeira, ela retornará o valor especificado. Execute a query e visualize os resultados. Observe que a nova coluna **order_amt_category** foi criada com os valores especificados.
-- MAGIC
-- MAGIC [expressão case](https://docs.databricks.com/aws/en/sql/language-manual/functions/case)

-- COMMAND ----------

-- DBTITLE 1, CASE WHEN
SELECT 
  orderid AS orders_id,
  customerid AS customer_id,
  productid AS product_id,
  quantity,
  orderamt AS order_amt,
  ROUND(order_amt/quantity,2) AS price_per_item,
  UPPER(salesrep) AS sales_rep,
  'Australia' AS order_country,
  orderdate AS order_date,
  YEAR(order_date) AS order_year,                             
  MONTH(order_date) AS order_month,                           
  DAY(order_date) AS order_day,                              
  DAYNAME(order_date) AS order_dayofweek,                     
  QUARTER(order_date) AS order_quarter,                       
  CONCAT(order_year,'Q',order_quarter) AS order_year_quarter,
  -- Crie uma nova coluna para order_amt_category com base nestas condições
  CASE                                                    
    WHEN order_amt <= 250000 THEN 'Low'
    WHEN order_amt > 250000 AND order_amt < 1000000 THEN 'Middle'
    ELSE 'High'
  END AS order_amt_category
  -- Fim da instrução CASE WHEN
FROM aus_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Olhando para os resultados acima, observe que temos o **product_id** para cada pedido. 
-- MAGIC
-- MAGIC     No entanto, esse valor não nos diz o nome específico do produto. Nosso objetivo é obter o **nome do produto** para cada pedido para identificar facilmente o produto para nossa análise downstream. Podemos fazer isso usando a tabela **au_products_lookup**.
-- MAGIC
-- MAGIC     Execute a query abaixo para visualizar a tabela **au_products_lookup**. Observe que a tabela contém as colunas **productid** e **productname**. Podemos usar a coluna **productid** de ambas as tabelas para executar um join para obter a coluna **productname**.

-- COMMAND ----------

SELECT *
FROM au_products_lookup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Vamos executar um INNER JOIN com a tabela **au_products_lookup** para obter a coluna **productname** usando a coluna **productid** de cada tabela.
-- MAGIC
-- MAGIC     Na query abaixo:
-- MAGIC     - A tabela **aus_orders** tem o alias **ord**, que é adicionado na frente das colunas para puxar explicitamente essas colunas da tabela.
-- MAGIC     - A tabela **au_products_lookup** tem o alias **prod**, que é adicionado na frente de **productname** para selecionar essa coluna da tabela.
-- MAGIC     - O join ocorre nas colunas **ord.productid** e **prod.productid**.
-- MAGIC
-- MAGIC
-- MAGIC     **Resumo dos joins:**
-- MAGIC     - **INNER JOIN**: Retorna apenas linhas correspondentes de ambas as tabelas.
-- MAGIC     - **LEFT JOIN**: Retorna todas as linhas da tabela esquerda e as linhas correspondentes da tabela direita (nulos quando não há correspondência).
-- MAGIC     - **RIGHT JOIN**: Retorna todas as linhas da tabela direita e as linhas correspondentes da tabela esquerda (nulos quando não há correspondência).
-- MAGIC     - **FULL JOIN**: Retorna todas as linhas de ambas as tabelas, com nulos onde não há correspondência.
-- MAGIC
-- MAGIC     Para obter mais detalhes, consulte a [documentação do Databricks JOIN](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-join).

-- COMMAND ----------

-- DBTITLE 1, INNER JOIN
SELECT 
  ord.orderid AS orders_id,
  ord.customerid AS customer_id,
  ord.productid AS product_id,
  prod.productname AS product_name,            -- Obter a coluna productname da tabela au_products_lookup
  ord.quantity,
  ord.orderamt AS order_amt,
  ROUND(order_amt/quantity,2) AS price_per_item,
  UPPER(salesrep) AS sales_rep,
  'Australia' AS order_country,
  ord.orderdate AS order_date,
  YEAR(order_date) AS order_year,                             
  MONTH(order_date) AS order_month,                         
  DAY(order_date) AS order_day,                               
  DAYNAME(order_date) AS order_dayofweek,                     
  QUARTER(order_date) AS order_quarter,                       
  CONCAT(order_year,'Q',order_quarter) AS order_year_quarter,
  CASE                                                    
    WHEN order_amt <= 250000 THEN 'Low'
    WHEN order_amt > 250000 AND order_amt < 1000000 THEN 'Middle'
    ELSE 'High'
  END AS order_amt_category
FROM aus_orders ord
  INNER JOIN au_products_lookup prod          -- INNER JOIN com a tabela au_products_lookup
  ON ord.productid = prod.productid           -- Na coluna productid em cada tabela
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Agora que passamos pelos passos, vamos juntar tudo isso com uma instrução `CREATE TABLE` para criar a nova tabela **aus_orders_silver** que prepara nossos dados de acordo com nossos objetivos.
-- MAGIC
-- MAGIC     Usaremos a arquitetura medallion durante nossa preparação de dados. Lembre-se, a arquitetura medallion é uma abordagem de gestão de dados que organiza os dados em várias camadas (bronze, prata e ouro) para refinar e enriquecer progressivamente os dados brutos para análise e business intelligence.
-- MAGIC
-- MAGIC     - As tabelas **Bronze** contêm dados brutos e não processados ingeridos diretamente dos sistemas de origem.
-- MAGIC
-- MAGIC     - As tabelas **Pratas** armazenam dados limpos e transformados, normalmente com algum nível de agregação e melhorias de qualidade para análise.
-- MAGIC
-- MAGIC     - As tabelas **Ouro** contêm dados de alta qualidade, prontos para os negócios, otimizados para relatórios e analítica, muitas vezes contendo agregações ou métricas finais.
-- MAGIC
-- MAGIC
-- MAGIC ![Arquitetura Medallion](../Includes/images/medallion_architecture.png)

-- COMMAND ----------

-- DBTITLE 1, CRIAR TABELA
-- Remova a tabela se existir ao executar esta célula várias vezes durante uma demonstração
DROP TABLE IF EXISTS aus_orders_silver;


CREATE TABLE aus_orders_silver AS
SELECT 
  ord.orderid AS orders_id,
  ord.customerid AS customer_id,
  ord.productid AS product_id,
  prod.productname AS product_name,            -- Obtenha a coluna productname da tabela au_products_lookup
  ord.quantity,
  ord.orderamt AS order_amt,
  ROUND(order_amt/quantity,2) AS price_per_item,               -- Cálculo de preço por item
  UPPER(salesrep) AS sales_rep,                                -- Nomes dos representantes de vendas em letras maiúsculas
  'Australia' AS order_country,                                -- String de país
  ord.orderdate AS order_date,
  YEAR(order_date) AS order_year,                              -- Obtenha o ano
  MONTH(order_date) AS order_month,                            -- Obtenha o mês
  DAY(order_date) AS order_day,                                -- Obtenha o mês
  DAYNAME(order_date) AS order_dayofweek,                      -- Obtenha o dia da semana como uma STRING
  QUARTER(order_date) AS order_quarter,                        -- Obtenha o trimestre
  CONCAT(order_year,'-Q',order_quarter) AS order_year_quarter, -- Crie o ano e o trimestre: 2020-Q1
  -- Crie uma nova coluna para order_amt_category com base nestas condições
  CASE                                                    
    WHEN order_amt <= 250000 THEN 'Low'
    WHEN order_amt > 250000 AND order_amt < 1000000 THEN 'Middle'
    ELSE 'High'
  END AS order_amt_category
  -- Fim da instrução CASE WHEN
FROM aus_orders ord
  INNER JOIN au_products_lookup prod           -- Junte-se à tabela au_products_looup
  ON ord.productid = prod.productid;           -- No productid

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11. View a nova tabela **aus_orders_silver**. Observe que ele contém todas as nossas novas colunas.

-- COMMAND ----------

SELECT *
FROM aus_orders_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12. Conclua as etapas a seguir para exibir a linhagem da tabela **aus_orders_silver** usando o Catalog Explorer:
-- MAGIC
-- MAGIC     a. Na barra de navegação esquerda, selecione o ícone de catálogo ![ícone do catálogo](../Includes/images/catalog_icon.png).
-- MAGIC
-- MAGIC     b. Expanda o catálogo **dbacademy**.  
-- MAGIC
-- MAGIC     c. Expanda seu esquema **labuser**.  
-- MAGIC
-- MAGIC     d. Expanda **Tabelas**.  
-- MAGIC
-- MAGIC     e. Encontre a tabela **aus_orders_silver** que você criou e, à direita da tabela, selecione as reticências ![reticências](../Includes/images/ellipsis.png).  
-- MAGIC
-- MAGIC     f. Selecione **Abrir no Catalog Explorer**.  
-- MAGIC
-- MAGIC     g. Na barra de navegação, selecione **Linhagem**. Aqui, você pode ver as tabelas **Upstream**.  
-- MAGIC
-- MAGIC     h. Para exibir um gráfico interativo da linhagem de dados, clique em **Ver gráfico de linhagem**. Por default, um nível é exibido no gráfico. Clique no ícone Sinal de Mais em um nó para revelar mais conexões, se elas estiverem disponíveis.  
-- MAGIC
-- MAGIC     i. Feche o Catalog Explorer.
-- MAGIC
-- MAGIC
-- MAGIC <br></br>
-- MAGIC ![aus_orders_silver](../Includes/images/aus_orders_silver_lineage_image.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Análise de Dados
-- MAGIC Vamos analisar a tabela **aus_orders_silver** e responder a algumas perguntas básicas de negócios.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Comece mostrando a tabela **aus_orders_silver** e exibindo 10 linhas para pré-visualizar a tabela.
-- MAGIC

-- COMMAND ----------

SELECT *
FROM aus_orders_silver
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D1. Filter
-- MAGIC Execute filtragem e ordenação simples usando DBSQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Veja todos os pedidos onde:
-- MAGIC    - O **sales_rep** é *RAVI*.
-- MAGIC    - Os pedidos são classificados em ordem decrescente de **order_amt**.

-- COMMAND ----------

SELECT
  sales_rep,
  orders_id,
  customer_id,
  product_name,
  order_amt,
  order_amt_category
FROM aus_orders_silver
WHERE sales_rep = 'RAVI'
ORDER BY order_amt DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Veja todos os pedidos onde:
-- MAGIC    - O **sales_rep** é *RAVI* E o **customer_id** é *NS7598*.
-- MAGIC    - Os pedidos são classificados em ordem decrescente de **order_amt**.

-- COMMAND ----------

SELECT
  sales_rep,
  orders_id,
  customer_id,
  product_name,
  order_amt,
  order_amt_category
FROM aus_orders_silver
WHERE sales_rep = 'RAVI' AND
      customer_id = 'NS7598'
ORDER BY order_amt DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D2. Estatísticas resumidas simples

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Conte o número total de pedidos na tabela **aus_orders_silver**. Execute a query e visualize os resultados. Observe que temos 5.726 pedidos totais.

-- COMMAND ----------

SELECT COUNT(*) AS total_orders
FROM aus_orders_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Calcule as estatísticas resumidas para **order_amt**, incluindo os valores **máx**, **média** e **mín**. Execute a query e visualize os resultados. 
-- MAGIC
-- MAGIC     Observe que temos uma ampla gama de valores de pedidos, com um valor médio de pedidos de cerca de *196,266*.

-- COMMAND ----------

SELECT
  max(order_amt) AS max_order_amount,
  avg(order_amt) AS avg_order_amount, 
  min(order_amt) AS min_order_amount
FROM aus_orders_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D3. Agregações e visualizações

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Vamos visualizar o total de **order_amt** para cada **product_name** para ver qual produto tem o maior valor total de pedido na tabela e visualizar os resultados.
-- MAGIC
-- MAGIC    Conclua os passos a seguir para executar a query e criar uma visualização usando os resultados da query:
-- MAGIC
-- MAGIC       A. Execute a célula abaixo. Qual produto tem o maior valor total de pedido?
-- MAGIC
-- MAGIC       b. Na saída da célula, selecione o ícone **+**.
-- MAGIC
-- MAGIC       c. Em seguida, selecione **Visualização**.
-- MAGIC
-- MAGIC       d. Na tab **Geral**, você pode modificar algumas opções básicas. Deixe-os nas configurações default.
-- MAGIC
-- MAGIC       e. Selecione a tab **Eixo Y** e desmarque **Classificar Valores**.
-- MAGIC
-- MAGIC       ![Opções do Eixo Y](../Includes/images/yaxis_options_sort.png)
-- MAGIC
-- MAGIC       f. Selecione **Salvar**.
-- MAGIC
-- MAGIC       g. Na célula de saída abaixo, clique duas vezes em **Visualização** e altere o nome para **Vendas por Produto**.
-- MAGIC
-- MAGIC       h. Veja a visualização que você criou. Observe como é fácil criar visualizações no Databricks.
-- MAGIC
-- MAGIC <br></br>
-- MAGIC **Visualização Final**
-- MAGIC ![Final Visualization](../Includes/images/final_visualization_order_by_product.png)

-- COMMAND ----------

SELECT 
  product_name, 
  ROUND(SUM(order_amt),2) as total_order_amount
FROM aus_orders_silver
GROUP BY product_name
ORDER BY total_order_amount DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Vamos determinar o total de **order_amt** para cada **product_name** distribuído por **order_year_quarter** e visualizar os resultados de cada **product_name** em um gráfico de linhas. 
-- MAGIC
-- MAGIC    Conclua os seguintes passos:
-- MAGIC
-- MAGIC    a. Execute a célula abaixo para calcular os resultados. Observe que estamos usando backticks na query para limpar os nomes das colunas para a visualização.
-- MAGIC
-- MAGIC     **NOTA:** Você também pode modificar os rótulos dentro da interface do usuário **Visualização**.
-- MAGIC
-- MAGIC    b. Na saída da célula, selecione o ícone **+**.
-- MAGIC
-- MAGIC    c. Em seguida, selecione **Visualização**.
-- MAGIC
-- MAGIC    d. Na tab **Geral**, você pode modificar algumas opções básicas. Altere o **Tipo de visualização** para **Linha** e desmarque **Gráfico horizontal**.
-- MAGIC    <img src="../Includes/images/year_order_general_options.png" width="600" alt="General Options">
-- MAGIC
-- MAGIC    e. Veja a visualização. Observe que os rótulos **Nome do Produto** e **Trimestre do Ano** estão limpos.
-- MAGIC
-- MAGIC    f. Selecione a tab **Eixo Y**. É aqui que você pode modificar o rótulo de quantidade. Na opção **Nome**, insira *Valor Total Vendido* para limpar o rótulo do eixo Y.
-- MAGIC    <img src="../Includes/images/year_order_y_axis_label.png" width="600" alt="Y Axis Label">
-- MAGIC
-- MAGIC    g. Selecione **Salvar**. Observe que a visualização foi criada.
-- MAGIC
-- MAGIC    h. Usando o cursor, passe o rato sobre a visualização. Observe que ele é interativo.
-- MAGIC
-- MAGIC
-- MAGIC    <br></br>
-- MAGIC    **Visualização Final**
-- MAGIC    ![Visualização final](../Includes/images/final_visualization_yearquarter_totalsold_product_name.png)

-- COMMAND ----------

SELECT 
  order_year_quarter AS `Year Quarter`,
  product_name AS `Product Name`, 
  ROUND(SUM(order_amt),2) AS `Total Amount Sold`
FROM aus_orders_silver
GROUP BY 
  order_year_quarter,
  product_name
ORDER BY order_year_quarter, product_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Vamos determinar o valor total de vendas para cada representante de vendas e qual porcentagem das vendas totais da empresa cada representante de vendas contribui para.
-- MAGIC
-- MAGIC     Para resolver isso, completaremos o seguinte:
-- MAGIC
-- MAGIC     - Na cláusula `SELECT`, calcularemos o percentual da contribuição de cada representante dividindo o total de vendas do representante comercial (**total_sales**) pelo total de vendas de toda a empresa usando `SELECT SUM(order_amt) FROM aus_orders_silver`.
-- MAGIC
-- MAGIC     - Use uma cláusula `GROUP BY` com uma agregação `SUM` para calcular o **total_sales** para cada **sales_rep**.
-- MAGIC     
-- MAGIC     Execute a query e visualize os resultados. Observe que a coluna **percentage_of_sales** contém a porcentagem total de vendas para cada representante de vendas.

-- COMMAND ----------

SELECT 
  sales_rep,
  ROUND(SUM(order_amt),2) as sales_rep_total_sales,
  ROUND(sales_rep_total_sales / (SELECT SUM(order_amt) FROM aus_orders_silver),2) as percentage_of_sales
FROM aus_orders_silver
GROUP BY sales_rep
ORDER BY percentage_of_sales DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D4. Common Table Expression (CTE)
-- MAGIC As CTEs definem um conjunto de resultados temporário que você pode referenciar várias vezes no escopo de uma instrução SQL. Dependendo da query que você está executando, eles podem ajudar a tornar sua query mais legível, mais fácil de depurar e manter.
-- MAGIC
-- MAGIC Vamos reescrever o código do passo anterior usando um CTE simples.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Primeiro, vamos escrever a query para calcular o valor total das vendas. Observe que o resultado retorna uma linha com uma única coluna mostrando o total de vendas: 1,035,503,255.36.

-- COMMAND ----------

SELECT ROUND(SUM(order_amt),2) AS total
FROM aus_orders_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Em seguida, vamos incorporar a query acima em um CTE para separar a subquery dentro da cláusula `SELECT` que usamos para calcular a porcentagem de vendas para cada representante de vendas.
-- MAGIC
-- MAGIC     A query abaixo:
-- MAGIC
-- MAGIC     - Usa a cláusula `WITH` para criar um conjunto de resultados temporário chamado **TotalSalesQry** que calcula o total de vendas em todos os pedidos usando a query acima.
-- MAGIC
-- MAGIC     - Na query principal, referenciamos o resultado **TotalSales** (soma total de pedidos) dentro da cláusula usando a instrução `SELECT`: `ROUND(sales_rep_total_sales / (SELECT total FROM TotalSalesQry), 2) AS percentage_of_sales`.
-- MAGIC
-- MAGIC     Execute a célula e veja os resultados. Observe que os resultados são os mesmos.
-- MAGIC
-- MAGIC     **NOTA:** Para este exemplo simples, uma CTE pode não ser necessária, mas as CTEs podem ser muito mais complexas e ajudar a modularizar sua query, tornando-a mais legível e fácil de depurar.

-- COMMAND ----------

-- CTE para armazenar o total de vendas
WITH TotalSalesQry AS (
  SELECT ROUND(SUM(order_amt),2) AS total
  FROM aus_orders_silver
)
-- Query que utiliza o CTE TotalSales
SELECT 
    sales_rep,
    ROUND(SUM(order_amt), 2) AS sales_rep_total_sales,
    -- Utilize o CTE TotalSales acima
    ROUND(sales_rep_total_sales / (SELECT total FROM TotalSalesQry), 2) AS percentage_of_sales
FROM aus_orders_silver
GROUP BY sales_rep
ORDER BY percentage_of_sales DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D5. PIVOT
-- MAGIC E se você quiser reestruturar seus dados? No SQL, o operador PIVOT é usado para transformar linhas em colunas, permitindo reorganizar e resumir dados para facilitar a análise e a geração de relatórios. 
-- MAGIC
-- MAGIC Por exemplo, e se quisermos resumir o total de vendas de cada **sales_rep** por **order_year** no seguinte formato:
-- MAGIC | sales_rep | 2021        | 2022        | 2023        | 2024        |
-- MAGIC |-----------|-------------|-------------|-------------|-------------|
-- MAGIC | HARRY     | 1,894,401.56 | 14,919,971.27 | 19,191,059.66 | 13,700,168.75 |
-- MAGIC | ZURI      | 4,824,302.84 | 33,586,531.28 | 62,555,959.30 | 61,231,945.73 |
-- MAGIC | LESLIE    | 4,715,409.51 | 41,244,100.17 | 47,641,618.74 | 55,997,979.40 |
-- MAGIC | RAVI      | 5,436,493.42 | 90,624,752.06 | 92,317,450.30 | 100,601,239.93 |
-- MAGIC | RÓISÍN    | 4,698,899.42 | 94,268,067.91 | 100,327,746.54 | 115,555,931.50 |
-- MAGIC | LIAM      | 1,369,265.15 | 16,312,696.91 | 19,054,808.61 | 33,432,455.40 |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Vamos começar por fazer query nas colunas na tabela **aus_orders_silver** que queremos dinamizar. Observe que temos as colunas **sales_rep**, **order_amt** e **order_year**.

-- COMMAND ----------

SELECT
    sales_rep, 
    order_amt, 
    order_year
FROM aus_orders_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Agora vamos pivotar o conjunto de resultados com um CTE e o operador `PIVOT` para reestruturar a tabela. 
-- MAGIC
-- MAGIC     Na célula a seguir, a operação `PIVOT` usa:
-- MAGIC
-- MAGIC     - A instrução `ROUND(SUM(order_amt))` para arredondar e somar a coluna **order_amt** para cada **sales_rep**.
-- MAGIC
-- MAGIC     - A instrução `FOR order_year IN (2021, 2022, 2023, 2024)` dinamiza os dados transformando os valores **order_year** em colunas separadas para cada ano: *2021, 2022, 2023, 2024*.
-- MAGIC
-- MAGIC
-- MAGIC **NOTA:** Embora não seja obrigatório, o CTE ajuda a tornar o código mais legível.

-- COMMAND ----------

-- Criar CTE da tabela de origem para pivotar
WITH SourceTable AS (
    SELECT
        sales_rep, 
        order_amt, 
        order_year
    FROM 
        aus_orders_silver
)
-- Selecione tudo da tabela de origem (CTE) e rode a coluna order_year
SELECT *
FROM SourceTable
PIVOT (
    ROUND(SUM(order_amt),2)
    FOR order_year IN (2021, 2022, 2023, 2024)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Vamos concluir a mesma tarefa sem usar o CTE. Cada método produzirá os mesmos resultados.

-- COMMAND ----------

SELECT *
FROM (
    SELECT 
        sales_rep, 
        order_amt, 
        order_year
    FROM 
        aus_orders_silver
) AS SourceTable
PIVOT (
    ROUND(SUM(order_amt),2)
    FOR order_year IN (2021, 2022, 2023, 2024)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D6. Funções de janela
-- MAGIC As funções de janela no SQL permitem que você execute cálculos em um conjunto de linhas relacionadas à linha atual, sem recolher o conjunto de resultados. Eles são usados com a cláusula `OVER` para definir uma janela de linhas, permitindo operações como:
-- MAGIC - classificação
-- MAGIC - totais acumulados
-- MAGIC - médias móveis dentro de um intervalo especificado de dados
-- MAGIC - e mais
-- MAGIC
-- MAGIC Nosso objetivo é visualizar os 3 principais valores de vendas para cada **sales_rep**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Visualize as colunas **sales_rep** e **order_amt** na tabela **aus_orders_silver**.
-- MAGIC
-- MAGIC     Execute a query e visualize os resultados. Observe o seguinte:
-- MAGIC       - Na linha 1, **sales_rep** *HARRY* tem sua maior venda de *2466504.04*.
-- MAGIC       - Na linha 277, **sales_rep** *LESLIE* tem sua maior venda de *3363380,3*.

-- COMMAND ----------

SELECT 
    sales_rep, 
    order_amt
FROM aus_orders_silver
ORDER BY sales_rep, order_amt DESC
LIMIT 500;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. A célula a seguir usa uma função de janela para classificar o **order_amt** para cada **sales_rep**.
-- MAGIC
-- MAGIC      - A função `RANK()` classifica cada registro dentro da partição especificada (**sales_rep**).
-- MAGIC
-- MAGIC      - A palavra-chave `OVER` é usada com funções de janela para definir a janela para a função operar. Ele especifica como particionar e/ou ordenar os dados.
-- MAGIC
-- MAGIC      - A cláusula `PARTITION BY` divide os dados em grupos por **sales_rep**.
-- MAGIC
-- MAGIC      - A cláusula `ORDER BY` ordena as linhas em ordem decrescente **order_amt**.
-- MAGIC
-- MAGIC     Execute a célula e veja os resultados. Observe o seguinte:
-- MAGIC       - A partir da linha 1, você pode ver o **sale_rank** de **sales_rep** *HARRY*.
-- MAGIC       - A partir da linha 277, o ranking é redefinido para o novo **sales_rep** *LESLIE*.
-- MAGIC
-- MAGIC     [Funções de janela](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions)

-- COMMAND ----------

SELECT 
    sales_rep, 
    order_amt,
    RANK() OVER(PARTITION BY sales_rep ORDER BY order_amt DESC) AS sale_rank
FROM aus_orders_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Por fim, queremos filtrar para as 3 maiores vendas. Para fazer isso, você pode usar a cláusula `QUALIFY`. A cláusula `QUALIFY` é usada em SQL para filtrar os resultados de funções de janela, como `RANK()`, depois de calculadas.
-- MAGIC
-- MAGIC     Execute a célula e veja os resultados. Observe que podemos ver os três primeiros **sale_rank** para cada **sales_rep**.
-- MAGIC
-- MAGIC     [Cláusula QUALIFY](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-qualify)
-- MAGIC
-- MAGIC     **NOTA:** Não é possível usar a `WHERE` cláusula para filtrar uma coluna criada pela função de janela.

-- COMMAND ----------

SELECT 
  sales_rep, 
  order_amt,
  RANK() OVER (PARTITION BY sales_rep ORDER BY order_amt DESC) AS sale_rank
FROM aus_orders_silver
QUALIFY sale_rank <=3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Recursos adicionais
-- MAGIC
-- MAGIC - [Funções integradas](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin)
-- MAGIC
-- MAGIC - [O que são funções definidas pelo usuário (UDFs)?](https://docs.databricks.com/aws/en/udf/)
-- MAGIC
-- MAGIC - [Funções da janela](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions)
-- MAGIC
-- MAGIC - [CREATE FUNCTION (SQL e Python)](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function)
-- MAGIC
-- MAGIC - [Aplicar IA em dados usando funções de IA do Databricks](https://docs.databricks.com/aws/en/large-language-models/ai-functions) - Pré-visualização pública a partir de 2025 1º trimestre

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
