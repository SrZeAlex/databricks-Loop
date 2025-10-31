-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2.2 Demo - Exploração Programática e Ingestão de Dados para o Unity Catalog
-- MAGIC
-- MAGIC Nesta demonstração, exploraremos programaticamente nossos objetos de dados, exibiremos um arquivo CSV bruto de um volume, leremos o arquivo CSV do volume e criaremos uma tabela.
-- MAGIC
-- MAGIC ### Objetivos
-- MAGIC - Aplicar técnicas programáticas para visualizar objetos de dados em nosso ambiente.
-- MAGIC - Demonstrar como fazer upload de um arquivo CSV em um volume no Databricks.
-- MAGIC - Demonstrar como usar uma `CREATE TABLE` instrução para criar uma tabela a partir de um arquivo CSV em um volume do Databricks.

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
-- MAGIC **NOTA:** O objeto`DA` é usado apenas em cursos da Databricks Academy e não está disponível fora desses cursos. Ele fará referência dinâmica à informação necessária para executar o curso no ambiente de laboratório.
-- MAGIC
-- MAGIC ### Informações importantes do LABORATÓRIO
-- MAGIC
-- MAGIC Lembre-se de que a configuração do laboratório é criada com o [0 - OBRIGATÓRIO - Configuração do curso e descobrimento de dados]($../0 - REQUIRED - Course Setup and Data Discovery). Se você encerrar a sessão de laboratório ou se a sessão atingir o limite de tempo, seu ambiente será redefinido e você precisará executar novamente a Configuração do curso do Notebook.

-- COMMAND ----------

-- MAGIC %run ../Includes/2.2-Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Explorando programaticamente seu ambiente
-- MAGIC
-- MAGIC Nesta seção, demonstraremos como explorar programaticamente seu ambiente, um método alternativo ao uso do Catalog Explorer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Exiba os catálogos disponíveis usando a instrução `SHOW CATALOGS`. Observe que seu ambiente tem uma série de catálogos disponíveis.

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Execute a célula a seguir para exibir o catálogo e o esquema default. Você deve observar que seu catálogo default está definido como **samples** e seu esquema default está definido como **nyctaxi**.
-- MAGIC
-- MAGIC    **NOTA:** Definir o catálogo e o esquema default no Databricks permite que você evite digitar repetidamente o caminho completo (catalog.schema.table) ao fazer referência aos objetos de dados. Uma vez definido, o Databricks usará automaticamente o catálogo e o esquema escolhidos, tornando mais fácil e rápido trabalhar com seus dados sem precisar especificar o namespace completo a cada vez.
-- MAGIC

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Use a instrução `SHOW SCHEMAS` para exibir esquemas disponíveis no catálogo default. Observe que ele exibe esquemas (bancos de dados) dentro do catálogo **samples**, já que esse é o catálogo default atual.

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Você pode modificar a instrução `SHOW SCHEMAS` para especificar um catálogo específico, como o catálogo **databrickstheloop**. Observe que isso exibe esquemas disponíveis (bancos de dados) dentro do catálogo **databrickstheloop**.

-- COMMAND ----------

SHOW SCHEMAS IN databrickstheloop;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Para ver tabelas disponíveis em um esquema, use a instrução `SHOW TABLES`. Observe que, por default, ele exibe uma tabela dentro do catálogo default **samples** no esquema **nyctaxi** (banco de dados).
-- MAGIC

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Para fazer query a tabela **samples.nyctaxi.trips**, você só precisa especificar o nome da tabela **trips** e não o namespace inteiro de três níveis (catalog.schema.table) porque o catálogo e o esquema default são **samples** e **nyctaxi**, respectivamente.

-- COMMAND ----------

SELECT *
FROM trips
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Vamos tentar fazer query da tabela **databrickstheloop.username.ca_orders** sem usar o namespace de três níveis. Observe que um erro é retornado porque ele está procurando a tabela **ca_orders** em **samples.nyctaxi**.

-- COMMAND ----------

-- Esta query irá retornar um erro 
-- Isto deve-se ao facto de a tabela não existir no catálogo default (samples) e no esquema default (nyctaxi)
SELECT *
FROM ca_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Queremos modificar nosso catálogo default e esquema default para usar **databrickstheloop** e nosso esquema **username** para evitar escrever o namespace de três níveis toda vez que fazemos query e criarmos tabelas neste curso.
-- MAGIC
-- MAGIC     No entanto, antes de prosseguirmos, observe que cada um de nós tem um nome de esquema diferente. O nome do esquema específico foi armazenado dinamicamente na variável SQL `DA.schema_name` durante o script de configuração da sala de aula.
-- MAGIC
-- MAGIC     Execute o código abaixo e confirme se o valor da variável `DA.schema_name` corresponde ao seu nome de esquema específico (por exemplo, **username**).

-- COMMAND ----------

values(DA.schema_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Vamos modificar nosso catálogo e esquema default usando as instruções `USE CATALOG` e `USE SCHEMA`.
-- MAGIC
-- MAGIC      - `USE CATALOG` – Define o catálogo atual.
-- MAGIC
-- MAGIC      - `USE SCHEMA` – Define o esquema atual. 
-- MAGIC
-- MAGIC     **NOTA:** Como o nome do esquema dinâmico é armazenado na variável `DA.schema_name` como uma cadeia de caracteres, precisaremos usar a cláusula `IDENTIFIER` para interpretar a cadeia de caracteres constante em nossa variável como um nome de esquema. A cláusula `IDENTIFIER` pode interpretar uma cadeia de caracteres constante como qualquer uma das seguintes:
-- MAGIC     - Nome da relação (tabela ou view)
-- MAGIC     - Nome da função
-- MAGIC     - Nome da coluna
-- MAGIC     - Nome do campo
-- MAGIC     - Nome do esquema
-- MAGIC     - Nome do catálogo
-- MAGIC
-- MAGIC     [Documentação da cláusula IDENTIFICADOR](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names-identifier-clause?language=SQL)
-- MAGIC
-- MAGIC   Como alternativa, você pode simplesmente adicionar o nome do esquema sem usar a cláusula `IDENTIFIER`.

-- COMMAND ----------

USE CATALOG databrickstheloop;
USE SCHEMA IDENTIFIER(DA.schema_name);

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Vamos ver as tabelas disponíveis no catálogo **databrickstheloop** dentro do nosso esquema **username**. Observe que o esquema contém uma variedade de tabelas.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11. Vamos query a tabela **ca_orders** no catálogo **databrickstheloop** dentro do nosso esquema **username** sem usar o namespace de três níveis.

-- COMMAND ----------

SELECT *
FROM ca_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12. Embora você possa definir seu catálogo e esquema default para evitar o uso do namespace de três níveis, há momentos em que convém fazer referência a uma tabela específica. Você pode fazer isso especificando o catálogo e o nome do esquema na query.
-- MAGIC
-- MAGIC     Neste exemplo, vamos fazer query da tabela **samples.nyctaxi.trips** usando o namespace de três níveis.

-- COMMAND ----------

SELECT *
FROM samples.nyctaxi.trips
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 13. Por fim, vamos visualizar os volumes disponíveis em nosso esquema **username** no catálogo **databrickstheloop** usando a instrução `SHOW VOLUMES`. Observe que nosso esquema **username** contém uma variedade de volumes do Databricks, incluindo o volume **backup**.

-- COMMAND ----------

SHOW VOLUMES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 14. Vamos visualizar os arquivos disponíveis em nosso volume **databrickstheloop.username.backup** usando a interface do usuário. Preencha o seguinte:
-- MAGIC
-- MAGIC     a. No painel de navegação esquerdo, expanda o catálogo **databrickstheloop**.
-- MAGIC
-- MAGIC     b. Expanda seu esquema **username**.
-- MAGIC
-- MAGIC     c. Expandir **Volumes**.
-- MAGIC
-- MAGIC     d. Expanda o volume **backup**.
-- MAGIC
-- MAGIC     e. Observe que o volume contém o arquivo **au_products.csv**.
-- MAGIC
-- MAGIC   **NOTAS:** 
-- MAGIC   - Você também pode usar a [instrução LIST](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-list)  para visualizar programaticamente os arquivos disponíveis em um volume mostrado abaixo.
-- MAGIC   - A instrução `LIST` não lista arquivos em tabelas gerenciadas do Unity Catalog.
-- MAGIC   - Adicione o nome do esquema exclusivo à cadeia de caracteres na instrução `LIST` para visualizar programaticamente o arquivo no volume.

-- COMMAND ----------

-- Adicione o nome do seu esquema na instrução LIST abaixo, exemplo - `/Volumes/databrickstheloop/samantha_cruz/backup`
LIST '/Volumes/databrickstheloop/<ADD_SCHEMA_NAME>/backup'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Criar uma tabela a partir de um arquivo CSV em um volume
-- MAGIC
-- MAGIC Nesta seção, usaremos SQL para criar uma tabela (Tabela Delta) a partir de um arquivo CSV armazenado em um volume do Databricks usando dois métodos:
-- MAGIC - `read_files`
-- MAGIC - `COPY INTO`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Nosso objetivo é ler o arquivo **au_products.csv** e criar uma tabela. Para começar, é recomendável examinar o(s) arquivo(s) bruto(s) que você deseja usar para criar uma tabela. Podemos fazer isso com o código a seguir para [query dos dados por caminho](https://docs.databricks.com/aws/en/query#query-data-by-path). Isso nos permite ver:
-- MAGIC
-- MAGIC     - O delimitador do arquivo CSV
-- MAGIC
-- MAGIC     - A estrutura geral do arquivo CSV
-- MAGIC
-- MAGIC     - Se o CSV contiver cabeçalhos na primeira linha
-- MAGIC     
-- MAGIC     - Você pode usar essa técnica para fazer query de uma variedade de tipos de arquivos.
-- MAGIC
-- MAGIC     Preencha o seguinte:
-- MAGIC
-- MAGIC       *  No painel de navegação esquerdo, navegue até o volume **backup** e localize o arquivo **au_products.csv**.
-- MAGIC
-- MAGIC       *  Na célula abaixo, coloque o cursor entre os dois backticks.
-- MAGIC
-- MAGIC       *  No painel de navegação, passe o rato sobre o arquivo **au_products.csv** e selecione o `>>` para inserir o caminho do arquivo CSV entre os backticks onde diz 'REPLACE WITH YOUR VOLUME PATH'. O seu terá a seguinte aparência com o nome exclusivo do seu esquema:
-- MAGIC
-- MAGIC
-- MAGIC       ```SQL`
-- MAGIC       SELECT *
-- MAGIC       FROM text.`/Volumes/databrickstheloop/username/backup/au_products.csv`
-- MAGIC       ```
-- MAGIC
-- MAGIC       *  Execute a célula e visualize os resultados. Observe que:
-- MAGIC          - A primeira linha do arquivo CSV contém um cabeçalho
-- MAGIC          - Os valores são separados por vírgula

-- COMMAND ----------

-- Adicione o caminho do volume ao seu ficheiro: Exemplo - `/Volumes/databrickstheloop/samantha_cruz/backup/au_products.csv`
SELECT *
FROM text.`/Volumes/databrickstheloop/samantha_cruz/backup/au_products.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Função read_files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Na célula abaixo, vamos criar uma tabela chamada **au_products** no catálogo **databrickstheloop** dentro do seu esquema **username** usando o arquivo **au_products.csv** com a [table-valued function read_files](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files) (TVF).
-- MAGIC
-- MAGIC     A função `read_files` lê arquivos de um local fornecido e retorna os dados em formato tabular. Ele suporta a leitura de formatos de arquivo JSON, CSV, XML, TEXT, BINARYFILE, PARQUET, AVRO e ORC.
-- MAGIC
-- MAGIC     A função `read_files` abaixo usa as seguintes opções:
-- MAGIC
-- MAGIC       - O caminho do arquivo CSV é criado concatenando o caminho do volume com o nome do esquema, que é armazenado na variável `DA.schema_name`. Isso permite a referência dinâmica do arquivo para o nome exclusivo do esquema de laboratório.
-- MAGIC       
-- MAGIC       - A opção `format => 'csv'`  especifica o formato do arquivo de dados no caminho de origem. O formato é inferido automaticamente se não for fornecido.
-- MAGIC       
-- MAGIC       - A opção `header => true` especifica que o arquivo CSV contém um cabeçalho.
-- MAGIC       
-- MAGIC       - Quando o esquema não é fornecido, `read_files` tenta inferir um esquema unificado através dos arquivos descobertos, o que requer a leitura de todos os arquivos.
-- MAGIC
-- MAGIC **NOTAS:**
-- MAGIC
-- MAGIC - Há uma variedade de opções diferentes para cada tipo de arquivo. Você pode ver as opções disponíveis para seu tipo de arquivo específico na documentação [Opções](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files#options) .
-- MAGIC
-- MAGIC - Se um volume contiver arquivos relacionados, você poderá ler todos os arquivos em uma tabela especificando o caminho do volume sem o nome do arquivo. Neste exemplo, estamos especificando o caminho do volume e o nome do arquivo.

-- COMMAND ----------

CREATE OR REPLACE TABLE au_products AS
SELECT *
FROM read_files(
  '/Volumes/databrickstheloop/' || DA.schema_name || '/backup/au_products.csv',
  format => 'csv',
  header => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Visualize as tabelas disponíveis em seu esquema **username**. Observe que uma nova tabela chamada **au_products** foi criada.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Query a tabela **au_products** dentro do seu esquema **username** e visualize os resultados.
-- MAGIC
-- MAGIC     Observe o seguinte:
-- MAGIC     - A tabela foi criada a partir do arquivo CSV com sucesso.
-- MAGIC     - Uma nova coluna chamada **_rescued_data** foi adicionada. Essa coluna é fornecida por default para resgatar dados que não correspondem ao esquema.

-- COMMAND ----------

SELECT *
FROM au_products;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Quando o esquema não é fornecido, `read_files`  tenta inferir um esquema unificado através dos arquivos descobertos, o que requer a leitura de todos os arquivos que podem ser ineficientes para arquivos grandes.
-- MAGIC
-- MAGIC     Para arquivos maiores, é mais eficiente especificar o esquema dentro da função `read_files`. Para o nosso pequeno arquivo CSV, o desempenho não é um problema.

-- COMMAND ----------

CREATE OR REPLACE TABLE au_products_with_schema AS
SELECT *
FROM read_files(
  '/Volumes/databrickstheloop/' || DA.schema_name || '/backup/au_products.csv',
  format => 'csv',
  header => true,
  schema => '
    productid STRING,
    productname STRING,
    listprice DOUBLE
  '
);

SELECT *
FROM au_products_with_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. COPY INTO
-- MAGIC Outro método para criar uma tabela a partir de arquivos é usando a instrução `COPY INTO`. A instrução `COPY INTO` carrega dados de um local de arquivo em uma tabela Delta. Esta é uma operação repetível e idempotente — Os arquivos no local de origem que já foram carregados são ignorados. Isso é verdadeiro mesmo se os arquivos tiverem sido modificados desde que foram carregados.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. A célula abaixo mostra como criar uma tabela e copiar dados CSV para ela usando `COPY INTO`:
-- MAGIC
-- MAGIC    a. A instrução `CREATE TABLE` cria uma tabela vazia com um esquema definido (colunas `productid`, `productname` e `listprice`). `COPY INTO` copiará os dados para esta tabela.
-- MAGIC
-- MAGIC    b. O comando `COPY INTO` carrega arquivos CSV do caminho especificado na tabela criada. 
-- MAGIC
-- MAGIC    c. `FROM` especifica o caminho do volume a ser lido. Isso também pode fazer referência a um local externo.
-- MAGIC
-- MAGIC     - O `FILEFORMAT = CSV` especifica o formato dos dados de entrada. O Databricks suporta vários formatos de arquivo, como CSV, PARQUET, JSON e muito mais.
-- MAGIC     
-- MAGIC     - O `FORMAT_OPTIONS` especifica opções de formato de arquivo, como leitura do cabeçalho e inferência de esquema.
-- MAGIC
-- MAGIC <br></br>
-- MAGIC
-- MAGIC **OBRIGATÓRIO** - Na célula abaixo, adicione o caminho para o volume YOUR **backup** na cláusula `FROM` de `COPY INTO`. 
-- MAGIC
-- MAGIC Exemplo: `FROM '/Volumes/databrickstheloop/username/backup'`

-- COMMAND ----------

DROP TABLE IF EXISTS au_products_copy_into;


-- Crie uma tabela vazia
-- Pode definir o esquema da tabela se desejar
CREATE TABLE au_products_copy_into(
  productid STRING,
  productname STRING,
  listprice DOUBLE
);


-- Copie os ficheiros para a tabela e junte o esquema
COPY INTO au_products_copy_into
FROM 'REPLACE WITH THE PATH TO YOUR backup VOLUME'     -- TO DO: Adicione o seu caminho para o volume de cópia de segurança aqui
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Execute novamente a instrução `COPY INTO` depois de adicionar o caminho ao volume de **backup** na cláusula `FROM` novamente. 
-- MAGIC
-- MAGIC     Observe que **num_affected_rows** e **num_inserted_rows** são ambos 0. Como todos os dados já foram lidos, `COPY INTO`  não ingere o(s) arquivo(s) novamente.

-- COMMAND ----------

COPY INTO au_products_copy_into
FROM 'REPLACE WITH THE PATH TO YOUR backup VOLUME'   -- TO DO: Adicione o seu caminho para o volume de cópia de segurança aqui
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Exiba a tabela **au_products_copy_into**.

-- COMMAND ----------

SELECT *
FROM au_products_copy_into;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Resumo: COPY INTO (legacy)
-- MAGIC O comando CREATE STREAMING TABLE SQL é a alternativa recomendada ao comando legacy COPY INTO SQL para ingestão incremental do armazenamento de objetos em nuvem. Consulte COPY INTO. Para uma experiência de ingestão de arquivos mais escalável e robusta, o Databricks recomenda que os usuários de SQL utilizem tabelas de transmissão em vez de COPY INTO.
-- MAGIC
-- MAGIC [COPY INTO (legacy)](https://docs.databricks.com/aws/en/ingestion/#copy-into-legacy)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C3. Introdução às tabelas de transmissão no DBSQL (Bônus)
-- MAGIC
-- MAGIC Esta é uma introdução de alto nível às tabelas de transmissão no DBSQL. As tabelas de transmissão permitem transmissão ou processamento incremental de dados. Dependendo do seu objetivo final, as tabelas de transmissão podem ser extremamente úteis. 
-- MAGIC
-- MAGIC
-- MAGIC   Abordaremos brevemente o tópico para familiarizá-lo com suas capacidades. Para obter mais detalhes, consulte a documentação [CREATE STREAMING TABLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table). O Databricks oferece vários recursos para gerenciar transmissão e ingestão de dados em tempo real, portanto, lembre-se de consultar sua equipe de Engenharia de Dados para obter suporte adicional com dados de transmissão.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Neste exemplo, criaremos uma tabela de transmissão prata a partir de uma tabela bronze simples. Em muitos cenários, você estará lendo a partir do armazenamento em nuvem para iniciar o processo. Se você quiser transmitir dados brutos para uma tabela a partir do armazenamento em nuvem, veja exemplos na documentação [CREATE STREAMING TABLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table).
-- MAGIC
-- MAGIC     Para começar, vamos criar a tabela **emp_bronze_raw** com uma lista de funcionários.

-- COMMAND ----------

-- Utilize o nosso catálogo e esquema
USE CATALOG databrickstheloop;
USE SCHEMA IDENTIFIER(DA.schema_name);

-- Remova as tabelas se existirem para começar do início
DROP TABLE IF EXISTS emp_bronze_raw;
DROP TABLE IF EXISTS emp_silver_streaming;

-- Crie a tabela de funcionários
CREATE TABLE emp_bronze_raw (
    EmployeeID INT,
    FirstName VARCHAR(20),
    Department VARCHAR(20)
);

-- Introduza 5 linhas de dados de amostra
INSERT INTO emp_bronze_raw (EmployeeID, FirstName, Department)
VALUES
(1, 'John', 'Marketing'),
(2, 'Raul', 'HR'),
(3, 'Michael', 'IT'),
(4, 'Panagiotis', 'Finance'),
(5, 'Aniket', 'Operations');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Use a instrução `CREATE OR REFRESH STREAMING TABLE` para criar uma tabela de transmissão chamada **emp_silver_streaming**. Essa tabela carregará incrementalmente novas linhas à medida que forem adicionadas à tabela **emp_bronze_raw**. Ele também criará uma nova coluna chamada **IngestDateTime**, que registra a data e a hora em que a linha foi ingerida.
-- MAGIC
-- MAGIC **NOTAS:**
-- MAGIC - Este processo levará cerca de um minuto para ser executado. Nos bastidores, as tabelas de transmissão criam um pipeline DLT. Abordaremos o DLT em detalhes mais adiante neste curso.
-- MAGIC - As tabelas de transmissão são suportadas apenas no DLT e no Databricks SQL com Unity Catalog.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE emp_silver_streaming 
SCHEDULE EVERY 1 HOUR     -- O agendamento da refresh é opcional
SELECT 
  *, 
  current_timestamp() AS IngestDateTime
FROM STREAM emp_bronze_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. A instrução `DESCRIBE HISTORY` exibe uma lista detalhada de todas as alterações, versões e metadados associados a uma tabela Delta, incluindo informações sobre atualizações, exclusões e alterações de esquema.
-- MAGIC
-- MAGIC     Execute a célula abaixo e veja os resultados. Observe o seguinte:
-- MAGIC
-- MAGIC     - Na coluna **operação**, você pode ver que uma tabela de transmissão executa duas operações: **DLT SETUP** e **STREAMING UPDATE**.
-- MAGIC     
-- MAGIC     - Role para a direita e encontre a coluna **operationMetrics**. Na linha 1 (Versão 2 da tabela), o valor mostra que o **numOutputRows** é 5, indicando que 5 linhas foram adicionadas à tabela **emp_silver_streaming**.
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY emp_silver_streaming;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Execute a célula abaixo para query a tabela **emp_silver_streaming**. Observe que os resultados exibem 5 linhas de dados.

-- COMMAND ----------

SELECT *
FROM emp_silver_streaming;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Execute a célula abaixo para inserir 2 linhas de dados na tabela originalmente chamada **emp_bronze_raw**.

-- COMMAND ----------

INSERT INTO emp_bronze_raw (EmployeeID, FirstName, Department)
VALUES
(6, 'Athena', 'Marketing'),
(7, 'Pedro', 'Training');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Em nosso cenário, a atualização agendada ocorre a cada hora. No entanto, não queremos esperar tanto tempo. Use a instrução `REFRESH STREAMING TABLE` para atualizar a tabela de transmissão **emp_silver_streaming**. Essa instrução atualiza os dados de uma tabela de transmissão (ou de uma view materializada, que será abordada posteriormente).
-- MAGIC
-- MAGIC
-- MAGIC     Para obter mais informações sobre como refresh uma tabela de transmissão, consulte a documentação [REFRESH (MATERIALIZED VIEW ou tabela de streaming)](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-refresh-full).

-- COMMAND ----------

REFRESH STREAMING TABLE emp_silver_streaming;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Execute a instrução `DESCRIBE HISTORY` novamente para exibir as alterações na tabela.
-- MAGIC
-- MAGIC     Observe o seguinte:
-- MAGIC     - A tabela **emp_silver_streaming** agora tem uma nova versão, a versão 3.
-- MAGIC
-- MAGIC     - Role até a coluna **operationParameters**. Observe que **outputMode** especifica que uma operação **Append** ocorreu.
-- MAGIC     
-- MAGIC     - Role até a coluna **operationMetrics**. Observe que o valor de **numOutputRows** é 2, indicando que ocorreu uma atualização incremental e que duas novas linhas foram adicionadas à tabela **emp_silver_streaming**.

-- COMMAND ----------

DESCRIBE HISTORY emp_silver_streaming;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Execute a query abaixo para visualizar os dados em **emp_silver_streaming**. Observe que a tabela agora contém 7 linhas.

-- COMMAND ----------

SELECT *
FROM emp_silver_streaming;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Por fim, vamos descartar as duas tabelas que criamos.

-- COMMAND ----------

DROP TABLE IF EXISTS emp_bronze_raw;
DROP TABLE IF EXISTS emp_silver_streaming;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
