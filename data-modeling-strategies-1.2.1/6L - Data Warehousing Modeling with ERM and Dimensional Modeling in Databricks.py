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
# MAGIC * Construir modelos dimensionais usando técnicas de Slowly Changing Dimension (SCD) Tipo 2
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
# MAGIC Uma vez que a biblioteca é instalada, reiniciamos o kernel Python para que ele esteja totalmente disponível.

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
# MAGIC Crie duas tabelas com restrições de **Primary Key (PK) e Foreign Key (FK):**:  
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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC Verifique o catálogo e a esquema atuais.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verificar catálogo atual
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verificar o esquema atual
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Passo 4: Inserir dados das tabelas bronze TPC-H
# MAGIC
# MAGIC **Tarefa:**  
# MAGIC Preencha as tabelas recém-criadas `lab_customer` e `lab_orders` com dados das tabelas TPC-H localizadas na esquema `bronze`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Inserir dados no lab_customer de bronze.customer
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Inserir dados no lab_orders de bronze.pedidos
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC **Tarefa 2:** Testar restrição de Primary Key  
# MAGIC
# MAGIC Insira uma linha duplicada em `lab_customer` usando um valor `c_custkey` que já existe na tabela. Como a Databricks não impõe restrições de primary key, verifique se a inserção é permitida sem erros.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Teste de restrição de Primary Key ----
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Remover a linha de primary key duplicada
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela refined_customer se ela ainda não existir
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela refined_orders se ela ainda não existir
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela DimCustomer para armazenar detalhes do cliente com atributos Slowly Changing Dimension (SCD) Tipo 2 para rastreamento histórico
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela Simple DimDate para armazenar informações relacionadas à data
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verificar catálogo atual
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verificar o esquema atual
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Alternar para a esquema prata
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Inserir dados transformados da tabela bronze.customer na tabela refined_customer
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Inserir dados transformados da tabela bronze.orders na tabela refined_orders
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 10: Validar os registros da tabela
# MAGIC **Tarefa:** Validar se os registros foram carregados com êxito nas tabelas `refined_customer` e `refined_orders`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela refined_customer
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela refined_orders
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela refined_customer
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela refined_orders
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros para a tabela DimDate
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros para a tabela FactOrders
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir o conteúdo da exibição temporária para verificar os dados
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query para exibir detalhes de um novo cliente com o ID do cliente 99999
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela DimDate com o nome da tabela
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir a contagem de registros na tabela FactOrders com o nome da tabela
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13.2 Exemplo de consulta: Principais Segmentos de Mercado
# MAGIC
# MAGIC **Tarefa:** Exibir o valor total gasto pelos clientes em cada segmento de mercado, limitado aos 10 principais segmentos.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Query para calcular o valor total gasto pelos clientes em cada segmento de mercado.
# MAGIC    - Use `SELECT market_segment, SUM(order_amount) AS total_spent FROM FactOrders f JOIN DimCustomer dc ON f.customer_id = dc.customer_id GROUP BY market_segment ORDER BY total_spent DESC LIMIT 10`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Exibir o valor total gasto pelos clientes em cada segmento de mercado, limitado aos 10 principais segmentos
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

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

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela Hub para o Cliente
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela Hub para pedidos
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.2 Criar tabelas de link
# MAGIC
# MAGIC **Tarefa:** Criar tabelas de link para representar relacionamentos entre entidades de negócios.
# MAGIC
# MAGIC Passos a executar:
# MAGIC
# MAGIC 1. Crie a tabela de `L_Customer_Order` link.
# MAGIC    - Esta tabela mapeia os clientes para seus pedidos usando keys compostas com hash.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Cria uma tabela de links para mapear clientes para seus pedidos com uma hashed primary key
# MAGIC ---- <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Criar a tabela satélite para armazenar as Informações Descritivas do Pedido
# MAGIC ---- <FILL-IN>

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

## Defina o catálogo default para seu catálogo principal e a esquema para o esquema prata no spark sql
## <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o catálogo atual para o nome do catálogo extraído
# MAGIC USE CATALOG IDENTIFIER(:catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina a esquema atual para o nome do esquema definido
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 15: Processo ETL
# MAGIC
# MAGIC **Tarefa:** Carregue dados refinados nas tabelas do Data Vault 2.0 seguindo uma abordagem ETL estruturada.
# MAGIC
# MAGIC As subetapas incluem:
# MAGIC 1. Carregando Hubs  
# MAGIC 2. Carregando links  
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

## Gere hash keys e colunas de hash diff para entidades do Data Vault, incluindo cliente, pedido e seus relacionamentos, usando MD5
## <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Cria uma tabela de fatos de pedidos refinados na camada prata
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC **Tarefa 2:** Definir e executar funções de carga ETL.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Defina funções de carga ETL para clientes e pedidos.
# MAGIC 2. Execute as funções de carregamento ETL para preparar dados para tabelas do Data Vault.

# COMMAND ----------

## Definindo funções de carga ETL
## <FILL-IN>

# COMMAND ----------

## Executar carga ETL
## <FILL-IN>

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

## Carregue e mescle novos registros de clientes na tabela do hub H_Customer com hash key e metadados
## <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Visualizar os primeiros 10 registros da tabela de hub do H_Customer
# MAGIC ---- <FILL-IN>

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

## Mescle novos registros descritivos de clientes na tabela satélite S_Customer com hash diff e metadados
## <FILL-IN>

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

## Carregue e mescle novos registros de pedidos em tabelas de hub e satélite com hash keys, hash diff e metadados
## <FILL-IN>

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

## Criar e mesclar registros de link do pedido do cliente com hash key e metadados combinados na tabela de links
## <FILL-IN>

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
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 17: Exemplo de consulta
# MAGIC
# MAGIC Passo 17: Exemplo de query
# MAGIC
# MAGIC **Tarefa:** Execute uma query de exemplo para demonstrar como usar a view de negócios criada a partir do modelo do Data Vault.
# MAGIC
# MAGIC Passos a executar:
# MAGIC 1. Query o conteúdo completo da view de negócios criada na etapa anterior.
# MAGIC 2. Calcula o total de vendas por cliente e apresenta resultados em ordem decrescente.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query o conteúdo completo da view de negócios criada na etapa anterior
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Calcule o total de vendas por cliente e exiba os resultados em ordem decrescente
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 18: Etapas de verificação
# MAGIC
# MAGIC **Tarefa:** Executar a verificação básica para garantir a integridade e a correção do modelo do Data Vault.
# MAGIC
# MAGIC Passos a executar:
# MAGIC
# MAGIC 1. Verifique o número de registros em cada tabela do Data Vault (Hubs, Links e Satélites).
# MAGIC 2. Verifique se cada pedido está associado a exatamente um cliente.
# MAGIC    - Contar pedidos com um e vários clientes associados para garantir a integridade da ligação.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verificar contagens de registros
# MAGIC ---- <FILL-IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verifique se cada pedido está associado a exatamente um cliente
# MAGIC ---- <FILL-IN>

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

## Gere recursos básicos no nível do cliente agregando dados do pedido e juntando com informações do cliente
## <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 19.1: Registrar ou mesclar-atualizar tabela de recursos
# MAGIC
# MAGIC **Tarefa:** Crie uma tabela de recursos nomeada `customer_features` na esquema ouro. Se a tabela já existir, mescle os novos dados.

# COMMAND ----------

## Criar ou mesclar-atualizar a tabela 'customer_features' com recursos agregados no nível do cliente
## <FILL-IN>

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

## Adicionar rótulo binário aos clientes com base em se seus gastos totais excedem um limite
## <FILL-IN>

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

## Crie um conjunto de treinamento pesquisando recursos do repositório de recursos e juntando-os com dados rotulados
## <FILL-IN>

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

## Construir e treinar um modelo de regressão logística usando um pipeline com etapas de transformação de características
## <FILL-IN>

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

## Selecione 5 clientes de amostra e adicione uma coluna de indicador para demonstração de inferência em lote
## <FILL-IN>

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

## Realize inferência em lotes recuperando recursos para clientes de amostra e aplicando o modelo treinado
## <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza
# MAGIC Agora, limpe seu ambiente de trabalho.
# MAGIC
# MAGIC Execute o script abaixo para excluir o catálogo `catalog_name` e todos os seus objetos, se ele existir.

# COMMAND ----------

# Exclua o catálogo junto com todos os objetos (esquemas, tabelas) dentro.
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
