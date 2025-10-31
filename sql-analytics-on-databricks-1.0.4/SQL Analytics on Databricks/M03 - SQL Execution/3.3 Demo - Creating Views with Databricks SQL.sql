-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3.3 Demonstração - Criando views com o Databricks SQL
-- MAGIC
-- MAGIC Nesta demonstração, exploraremos vários tipos de views no Databricks e suas aplicações. Primeiro, criaremos uma view padrão e analisaremos seu comportamento, em seguida construiremos uma view temporária e avaliaremos seu uso baseado em sessão. Em seguida, desenvolveremos uma view materializada para avaliar seu impacto no desempenho da query e, finalmente, construiremos uma view dinâmica para examinar como ela lida com atualizações de dados em tempo real para query contínua.
-- MAGIC
-- MAGIC ### Objetivos
-- MAGIC - Criar uma view padrão e analisar seu comportamento para entender como ela é definida, armazenada e queried no Databricks.
-- MAGIC - Criar uma view temporária e avaliar sua finalidade no processamento de dados baseado em sessão, aprendendo como ela difere das views padrão.
-- MAGIC - Criar uma view materializada e avaliar seu desempenho, armazenando resultados pré-computados e reduzindo o tempo de computação da query.
-- MAGIC - Criar uma view dinâmica e examinar como ela lida com dados em tempo real, permitindo atualizações contínuas para queries no Databricks.

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
-- MAGIC     - Na lista suspensa, verifique se **shared_warehouse** está selecionado.
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

-- MAGIC %run ../Includes/3.3-Classroom-Setup

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
  DA.schema_name AS variable_value -- Exibir valor da variável DA.schema_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Crie uma tabela de exemplo chamada **funcionários** com 5 linhas e 3 colunas para usar na demonstração.

-- COMMAND ----------

-- Remova a tabela se já existir para começar do início
DROP TABLE IF EXISTS employees;

-- Crie a tabela de funcionários
CREATE TABLE employees (
    EmployeeID INT,
    FirstName VARCHAR(20),
    Department VARCHAR(20)
);

-- Introduza 5 linhas de dados de amostra
INSERT INTO Employees (EmployeeID, FirstName, Department)
VALUES
(1, 'John', 'Marketing'),
(2, 'Raul', 'HR'),
(3, 'Michael', 'IT'),
(4, 'Panagiotis', 'Finance'),
(5, 'Aniket', 'Operations');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a query e veja a tabela **funcionários**. Confirme se ele contém 3 colunas e 5 linhas, com valores **EmployeeID** de 1 a 5.

-- COMMAND ----------

SELECT *
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Introdução aos tipos de View no Databricks SQL
-- MAGIC - B1. Views padrão
-- MAGIC - B2. Views temporárias
-- MAGIC - B3. Views materializadas
-- MAGIC - B4. Visualizações dinâmicas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. View Padrão
-- MAGIC
-- MAGIC
-- MAGIC Uma **view padrão** é essencialmente uma query salva no banco de dados. Ele não armazena dados fisicamente, mas define uma query que é executada toda vez que você acessa a view.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Use a instrução `CREATE OR REPLACE VIEW` para criar uma view padrão simples chamada **employees_vw**. Lembre-se de que uma view padrão armazena o texto de uma query dentro do nome da view.
-- MAGIC
-- MAGIC     Neste exemplo, a view simplesmente consultará todas as linhas da tabela **funcionários** e criará a nova coluna **ViewType** com o valor *View Padrão* quando executada. 
-- MAGIC     
-- MAGIC     Execute o código e veja os resultados. Observe que a view foi criada com êxito.

-- COMMAND ----------

CREATE OR REPLACE VIEW employees_vw AS
SELECT 
  *, 
  'Standard View' AS ViewType
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Execute o `SHOW TABLES` para visualizar todas as tabelas em seu esquema **labuser**. Observe que seu **employees_vw** é mostrado na lista de tabelas, mesmo que seja uma view, não uma tabela.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Use a instrução `SHOW VIEWS` para exibir somente as views em seu esquema **labuser**. Observe que ele retorna o seguinte sobre a(s) view(s):
-- MAGIC
-- MAGIC     - **namespace** - O nome do catálogo
-- MAGIC
-- MAGIC     - **viewName** - O nome da view.
-- MAGIC
-- MAGIC     - **isTemporary** - Se a view for temporária
-- MAGIC     
-- MAGIC     - **isMaterialized** - Se a view é uma view materializada.
-- MAGIC
-- MAGIC
-- MAGIC     Neste exemplo, o **employees_vw** é uma view padrão, não é temporário e não é materializado.

-- COMMAND ----------

SHOW VIEWS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Descreva a view **employees_vw**. Role até a parte inferior e observe que há linhas de informações adicionais da view. Explore as seguintes linhas na seção *#Informações Detalhadas da Tabela*:
-- MAGIC
-- MAGIC    - **Tipo**: Exibe o tipo da view.
-- MAGIC    - **Texto da View**: O texto da query da view.
-- MAGIC    - **Colunas de saída da query da view**: As colunas de saída criadas pela view.

-- COMMAND ----------

DESCRIBE TABLE EXTENDED employees_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Execute uma query na view **employees_vw**. 
-- MAGIC
-- MAGIC     Ao query uma view, ela executará o texto de query armazenado sempre que a view for queried. Neste exemplo, é uma view simples que faz query da todas as linhas da tabela **funcionários** e adiciona a coluna **ViewType**.
-- MAGIC     <br></br>
-- MAGIC     ```SQL`
-- MAGIC     SELECT *, 'Standard View' AS ViewType 
-- MAGIC     FROM employees
-- MAGIC     ```
-- MAGIC
-- MAGIC **NOTA:** Se a query que o modo de exibição armazenou consumir muitos recursos, isso pode causar problemas de desempenho se você estiver fazendo referência ao modo de exibição padrão várias vezes em todo o código, pois ele executará a query armazenada todas as vezes. Neste exemplo, a tabela é pequena e o desempenho não será um problema.

-- COMMAND ----------

SELECT * 
FROM employees_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Insira outra linha na tabela original de **funcionários**. A nova linha conterá um novo funcionário com **EmployeeID** 6.

-- COMMAND ----------

INSERT INTO employees (EmployeeID, FirstName, Department)
VALUES
(6, 'Athena', 'Marketing');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Query o **employees_vw**. A view executará a query armazenada e obterá os dados mais atualizados no **funcionários** e criará a coluna adicional **ViewType**.

-- COMMAND ----------

SELECT * 
FROM employees_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Resumo da View Padrão 
-- MAGIC ##### Benefícios:
-- MAGIC
-- MAGIC - **Queries sob demanda**: Use um view padrão quando os dados forem relativamente pequenos, a query for executada rapidamente e você vai querer os dados mais atualizados sempre que executar a query.
-- MAGIC
-- MAGIC - **\* Persistência da View**: Salva a view padrão em um catálogo e esquema, e a view pode ser compartilhada entre outros usuários.
-- MAGIC
-- MAGIC - **Dados Atualizados**: Quando você quiser sempre acessar os dados mais atualizados das tabelas subjacentes.
-- MAGIC
-- MAGIC - **Baixos requisitos de armazenamento**: Como views não armazenam dados, elas não exigem armazenamento adicional.
-- MAGIC
-- MAGIC ##### Considerações:
-- MAGIC - **Desempenho**: Toda vez que a exibição é queried, a **query subjacente precisa ser executada**, o que pode ser lento se o volume de dados for grande ou a query for complexa.
-- MAGIC
-- MAGIC - **\*Persistência da View**: Salva a exibição em um catálogo e esquema, portanto, se você precisar apenas da exibição temporariamente para sua sessão atual, ainda a salvará no esquema como um objeto.
-- MAGIC
-- MAGIC - **Sem Cache**: As views padrão não se beneficiam do cache, portanto, cada execução da view recalcula a query.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Views temporárias
-- MAGIC
-- MAGIC Uma view **temporária** é semelhante a uma view padrão, pois representa um resultado de query. No entanto, a principal diferença é que as views temporárias só existem durante a sessão ou o notebook. Eles são úteis para queries de curta duração ou análise exploratória de dados onde você não precisa salvar a view. 
-- MAGIC
-- MAGIC Existem dois tipos de views temporárias:
-- MAGIC
-- MAGIC - **TEMPORARY** - as views são visíveis apenas para a sessão que as criou e são descartadas quando a sessão termina.
-- MAGIC
-- MAGIC - **GLOBAL TEMPORARY** - as views estão vinculadas a um esquema temporário preservado pelo sistema global_temp. 
-- MAGIC
-- MAGIC   - **NOTA:** Não há suporte para views temporárias globais em um SQL warehouse.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Query a tabela **funcionários** e confirme se ela contém 6 linhas e 3 colunas.

-- COMMAND ----------

SELECT *
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Use a instrução `CREATE OR REPLACE TEMPORARY VIEW` para criar uma view temporária chamada **employees_temp_vw**.
-- MAGIC
-- MAGIC     A view temporária vai fazer query da todas as linhas da tabela **funcionários** e criará uma nova coluna chamada **ViewType** com o valor *Temp View*.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW employees_temp_vw AS
SELECT 
  *, 
  'Temp View' AS ViewType
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Executar a instrução `SHOW VIEWS` para visualizar as views disponíveis em seu esquema **labuser**.
-- MAGIC
-- MAGIC     Observe o seguinte para **employees_temp_vw**:
-- MAGIC     - A view temporária não tem um valor **namespace**, pois é temporária.
-- MAGIC     - A view temporária tem um valor *true* para a coluna **isTemporary**.
-- MAGIC     - A view temporária tem um valor de *false* para **isMaterialized**.
-- MAGIC
-- MAGIC **NOTAS:** 
-- MAGIC - Se o SQL warehouse for encerrado ou você desanexar e anexar novamente ao cluster, a view temporária será limpa.
-- MAGIC - Se você abrir outro notebook, a view temporária não estará disponível nessa sessão.

-- COMMAND ----------

SHOW VIEWS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Query a view temporária **employees_temp_vw** e observe que ela retorna todos os 6 funcionários com a nova coluna **ViewType**.

-- COMMAND ----------

SELECT *
FROM employees_temp_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Vamos adicionar outra linha (**EmployeeID** 7) à tabela original **funcionários** e visualizar a tabela. Confirme se a tabela **funcionários** contém 7 linhas e 3 colunas.

-- COMMAND ----------

INSERT INTO employees (EmployeeID, FirstName, Department)
VALUES
(7, 'Pedro', 'Training');

SELECT *
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. O que você acha que acontecerá quando executarmos novamente a view temporária? Será que:
-- MAGIC
-- MAGIC    - Só retornar as 6 linhas de dados originais quando a view temporária foi criada?
-- MAGIC
-- MAGIC    - Ou ele vai recalcular e obter os dados mais atualizados (7 linhas)?
-- MAGIC
-- MAGIC    Execute a query e visualize os resultados. Observe que, quando a view temporária é executada, ela obtém os dados mais atualizados porque executa a query armazenada na view temporária.

-- COMMAND ----------

SELECT *
FROM employees_temp_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Resumo da View temporária
-- MAGIC
-- MAGIC ##### Benefícios:
-- MAGIC
-- MAGIC - **View Não Persiste**: Views temporárias não são armazenadas no esquema, portanto, assim que a sessão termina, elas são descartadas automaticamente. Elas são ótimas se você só precisa da view para sua sessão.
-- MAGIC
-- MAGIC - **Exploratory Data Analysis** Ótimo para (EDA) onde você deseja testar ou inspecionar rapidamente diferentes transformações ou agregações sem a necessidade de armazená-las permanentemente.
-- MAGIC
-- MAGIC - **Resultados Intermediários**: Ótimo quando você precisa armazenar resultados intermediários temporariamente ao executar várias etapas de uma computação complexa, sem a necessidade de persistir os dados.
-- MAGIC
-- MAGIC - **Compartilhamento dentro de uma sessão**: Ótimo quando você deseja disponibilizar a query em várias queries na mesma sessão.
-- MAGIC
-- MAGIC ##### Considerações:
-- MAGIC
-- MAGIC - **Sessão Específica**: Visões temporárias são específicas da sessão, o que significa que não podem ser acessadas por outros usuários ou sessões.
-- MAGIC
-- MAGIC - **View Não Persiste**: Views temporárias não são armazenadas no esquema, portanto, assim que a sessão termina, elas são descartadas automaticamente.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B3. Views materializadas
-- MAGIC
-- MAGIC Uma **view materializada** é tabela gerenciada pelo Unity Catalog que permitem aos usuários pré-computar resultados com base na versão mais recente dos dados nas tabelas de origem. As views materializadas PODEM ser atualizadas periodicamente (ou sob demanda) com base em um mecanismo de refresh para obter os dados mais atualizados.
-- MAGIC
-- MAGIC As views materializadas no Databricks diferem de outras implementações, pois os resultados retornados refletem o estado dos dados quando a view materializada foi refreshed pela última vez, em vez de sempre atualizar os resultados quando a view materializada é queried. Você pode refresh manualmente views materializadas ou agendar refreshes.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Use a instrução `CREATE OR REPLACE MATERIALIZED VIEW` para criar a view materializada.
-- MAGIC
-- MAGIC    No Databricks SQL, as views materializadas são tabelas gerenciadas pelo Unity Catalog que permitem aos usuários pré-calcular resultados com base na versão mais recente dos dados nas tabelas de origem. Cada vez que uma view materializada é refreshed, os resultados da query são recalculados para refletir as alterações nos datasets upstream.
-- MAGIC
-- MAGIC    Execute a célula abaixo e veja os resultados. Observe que a saída retorna *A operação foi executada com êxito*.
-- MAGIC
-- MAGIC    **NOTAS:** 
-- MAGIC    - Esta célula levará cerca de um minuto para ser concluída.
-- MAGIC    - As views materializadas criadas no Databricks SQL são apoiadas por um pipeline DLT serverless. Seu espaço de trabalho deve oferecer suporte a pipelines serverless para usar essa funcionalidade.
-- MAGIC    - Para obter mais informações, visualize o [Use views materializadas no Databricks SQL](https://docs.databricks.com/aws/en/views/materialized). Esta demonstração terá rapidamente uma visão geral de uma view materializada.

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW employees_mv AS
SELECT 
  *,
  'Materialized View' AS ViewType
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Conclua o procedimento a seguir para usar a interface do usuário para exibir os objetos de view em seu esquema.
-- MAGIC
-- MAGIC    a. Na barra de navegação esquerda, selecione o ícone de catálogo ![Ícone do catálogo](../Includes/images/catalog_icon.png)
-- MAGIC    
-- MAGIC    b. Expanda o catálogo **dbacademy**.
-- MAGIC
-- MAGIC    c. Expanda seu esquema **labuser**.
-- MAGIC
-- MAGIC    d. Expanda **Tabelas**.
-- MAGIC
-- MAGIC    e. Role para baixo e encontre as views **employees_mv** e **employees_vw**. Olhe para os ícones. Observe que:
-- MAGIC
-- MAGIC       - Uma view materializada é armazenada em seu esquema.
-- MAGIC
-- MAGIC       - Os ícones diferem para uma view materializada (**employees_mv**) e uma view padrão (**employees_vw**).
-- MAGIC
-- MAGIC    ![View Icons](../Includes/images/mv_vs_view_icons.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Execute a instrução `SHOW VIEWS` para visualizar as views disponíveis em seu esquema **labuser**.
-- MAGIC
-- MAGIC    Observe o seguinte para a view materializada **employees_mv**:
-- MAGIC    - A view materializada foi armazenada em seu **namespace labuser**.
-- MAGIC    - A view materializada tem um valor *false* para a coluna **isTemporary**.
-- MAGIC    - A view materializada tem um valor de *true* para **isMaterialized**.

-- COMMAND ----------

SHOW VIEWS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Selecione todas as linhas da view materializada **employees_mv**. Execute a célula e veja os resultados. Observe que a view materializada retorna 7 linhas e 4 colunas.

-- COMMAND ----------

SELECT *
FROM employees_mv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Insira uma nova linha na tabela **funcionários** com o **EmployeeID** de 8. Execute a célula e exiba os resultados. Observe que a tabela **funcionários** agora tem 8 linhas e 3 colunas.

-- COMMAND ----------

INSERT INTO employees (EmployeeID, FirstName, Department)
VALUES
(8, "Dimitri", "IT");

SELECT *
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Selecione todas as linhas da view materializada **employees_mv**. Antes de executar a célula, quantas linhas você acha que a view irá mostrar? 
-- MAGIC
-- MAGIC       - As 7 linhas originais na tabela **funcionários** quando a view materializada foi criada?
-- MAGIC
-- MAGIC       - Ou as atuais 8 linhas na tabela **funcionários**?
-- MAGIC
-- MAGIC    Execute a célula e veja os resultados. Observe que o view materializada exibe as 7 linhas de dados que estavam presentes na tabela **funcionários** quando o view materializada foi criado. Isso ocorre porque ele pré-calcula os resultados e os armazena como um objeto para evitar o recálculo dos resultados cada vez que o view materializada é executada.
-- MAGIC
-- MAGIC    **NOTA:** O uso de uma view materializada é muito mais eficiente do que uma view padrão ou temporária quando a view consome muitos recursos. Isso ocorre porque cada vez que uma view padrão ou temporária é usada, os resultados devem ser computados. A desvantagem é que uma view materializada deve ser refreshed para obter os dados mais atualizados.

-- COMMAND ----------

SELECT *
FROM employees_mv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Para obter os dados mais atualizados da tabela **employees**, você deve refresh a view materializada. Você pode fazer isso usando a instrução `REFRESH MATERIALIZED VIEW`.
-- MAGIC
-- MAGIC    Execute a célula e veja os resultados. Isso vai fazer refresh dos dados da view materializada para obter os dados mais atualizados da tabela **funcionários**.
-- MAGIC
-- MAGIC     [REFRESH (MATERIALIZED VIEW ou documentação da STREAMING TABLE)](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-refresh-full)

-- COMMAND ----------

REFRESH MATERIALIZED VIEW employees_mv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Selecione todas as linhas da view materializada **employees_mv** refreshed. Execute a célula e veja os resultados. Quantas linhas você acha que serão exibidas, 7 ou 8?
-- MAGIC
-- MAGIC    Execute a célula e veja os resultados. Observe que, depois que a materialized view foi refreshed, agora temos os dados atualizados da tabela **funcionários** (todas as 8 linhas).

-- COMMAND ----------

SELECT *
FROM employees_mv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Em vez de fazer refresh manualmente uma materialized view, você pode agendar uma refresh ao criá-la. 
-- MAGIC
-- MAGIC
-- MAGIC     Por exemplo, se você quiser agendar a materialized view para ser refreshed a cada hora, poderá usar a instrução `SCHEDULE EVERY` ao criar a materialized view.
-- MAGIC
-- MAGIC     Para obter mais informações, consulte a documentação da instrução [CREATE MATERIALIZED VIEW](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view).

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW employees_mv_scheduled
  SCHEDULE EVERY 1 HOUR       -- Agende quando vai refresh a view.
AS
SELECT 
  *,
  'Materialized View' AS ViewType
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Use a instrução `DESCRIBE EXTENDED` para descrever a view materializada programada **employees_mv_scheduled**. Execute a célula e veja os resultados. Nos resultados, role até a parte inferior e localize as **# Informações de Refresh**. 
-- MAGIC
-- MAGIC
-- MAGIC       Observe o seguinte:
-- MAGIC
-- MAGIC       - Em **Informações de Refresh**, você pode ver que essa view materializada está programada para refresh (ou foi refreshed).
-- MAGIC       - Você pode ver informações de refresh, como **Última refresh, Tipo da Última Refresh, Estado da Refresh Mais Recente e Refresh Mais Recente**.
-- MAGIC       - A linha **Agenda de Refresh** indica com que frequência essa view materializada é refreshed. Neste exemplo, é a cada 1 hora.

-- COMMAND ----------

DESCRIBE EXTENDED employees_mv_scheduled;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Resumo de Visualizações Materializadas
-- MAGIC
-- MAGIC ##### Benefícios:
-- MAGIC
-- MAGIC - **Otimizações de Desempenho**: Quando a query subjacente é computacionalmente cara e você deseja pré-computar e armazenar os resultados, a query da view é mais rápida em seu código.
-- MAGIC   
-- MAGIC - **Dados mais recentes não obrigatórios**: Quando os dados na view não precisam ser atualizados em tempo real, mas devem ser atualizados periodicamente (por exemplo, diariamente ou de hora em hora).
-- MAGIC   
-- MAGIC - **Agregação de dados**: Se a view for uma agregação (por exemplo, resumos, médias, contagens) e o recálculo dessas agregações for demorado, as views materializadas poderão armazenar os resultados pré-computados.
-- MAGIC
-- MAGIC
-- MAGIC ##### Considerações:
-- MAGIC
-- MAGIC - **Armazenamento**: As views materializadas ocupam o armazenamento, pois armazenam os resultados da query em seu esquema.
-- MAGIC   
-- MAGIC - **Desatualização de dados**: Os dados podem ficar obsoletos dependendo do intervalo de refresh. Se os dados subjacentes mudam com frequência e você precisa de dados em tempo real, as views materializadas podem não ser ideais.
-- MAGIC   
-- MAGIC - **Manutenção**: Você precisa gerenciar a estratégia de refresh para a exibição materializada, que pode incluir refresh completas ou incrementais.
-- MAGIC
-- MAGIC Para obter mais informações, consulte [Usar views materializadas no Databricks SQL](https://docs.databricks.com/aws/en/views/materialized).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B4. Views dinâmicas
-- MAGIC
-- MAGIC No Unity Catalog, você pode usar **views dinâmicas** para configurar o controle de acesso refinado, incluindo:
-- MAGIC - Segurança em nível de coluna
-- MAGIC - Segurança em nível de linha
-- MAGIC - Mascaramento de dados (não abordado nesta demonstração)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Comece usando a função `is_account_group_member` para verificar se você é membro do grupo **admins**. Execute a célula e veja os resultados. Observe que ele retorna o valor *false*, indicando que você não faz parte do grupo **admins**.

-- COMMAND ----------

SELECT is_account_group_member('admins');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query da tabela **funcionários** e confirme se a tabela contém 8 linhas e as colunas **EmployeeID, FirstName e Departmento**.

-- COMMAND ----------

SELECT *
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Com uma view dinâmica, você pode limitar as colunas que um usuário ou grupo específico pode acessar. No exemplo a seguir, somente membros do grupo **admins** podem acessar a coluna **FirstName** da tabela **funcionários**. 
-- MAGIC
-- MAGIC     Para adicionar permissão em nível de coluna na query, use a instrução CASE para substituir pelo valor literal de string *Redacted* ou pelo conteúdo real da coluna **FirstName** com base em se o usuário que está executando a query está no grupo **admins**.

-- COMMAND ----------

-- DBTITLE 1,a. Permissões em nível de coluna
CREATE OR REPLACE VIEW employees_redact_name_col_dynamic_vw AS
SELECT
  EmployeeID,
  -- Redija uma coluna quando não fizer parte do grupo 'admins'
  CASE WHEN 
    -- Quando fizer parte do grupo 'admins' (true), devolva o FirstName       
    is_account_group_member('admins') THEN FirstName
    -- Se não fizer parte do grupo 'admins' (false), não poderá ver o Nome e ver 'Redacted'   
    ELSE 'Redacted'                                     
  END AS FirstName,
  Department
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Query a view **employees_redact_name_col_dynamic_vw**. Antes de executar a query, o que você acha que acontecerá? Lembre-se, você não faz parte do grupo **admins**.
-- MAGIC
-- MAGIC     Execute a célula e veja os resultados. Observe que, como você não faz parte do grupo **admins**, não é possível ver o **FirstName** do funcionário. 
-- MAGIC     
-- MAGIC     **NOTAS:** Com views dinâmicas, você pode limitar as colunas acessíveis a usuários ou grupos específicos, o que permite que você compartilhe essa view com outras pessoas e, com base em seu acesso, elas podem ver (ou não) colunas específicas.

-- COMMAND ----------

SELECT *
FROM employees_redact_name_col_dynamic_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Com uma view dinâmica, você também pode especificar permissões até o nível da linha. No exemplo a seguir, somente os membros do grupo **admins** podem ver todos os funcionários. Se você não faz parte do grupo **admin**, você só pode ver os funcionários no **Departamento** *IT*. 
-- MAGIC
-- MAGIC     Execute a célula abaixo e veja os resultados. Lembre-se de que você não faz parte do grupo **admins**, então você só pode acessar as linhas onde **Departmento** é *IT*.

-- COMMAND ----------

-- DBTITLE 1,b. Permissões em nível de linha
CREATE OR REPLACE VIEW employees_filter_dynamic_vw AS
SELECT * 
FROM employees
WHERE
  -- View dinâmica para filtrar na coluna Departamento
  CASE
    -- Quando um membro do grupo de administradores (true), pode ver todas as linhas
    WHEN is_account_group_member('admins') THEN TRUE
    -- Caso contrário, não faz parte do grupo de administradores (false), apenas pode ver as linhas de IT
    ELSE Department = 'IT'
  END;



-- Exibir o view
SELECT *
FROM employees_filter_dynamic_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Resumo de Views Dinâmicas
-- MAGIC
-- MAGIC ##### Benefícios:
-- MAGIC - **Controle de acesso**: Pode usar views dinâmicas para configurar o controle de acesso granular.
-- MAGIC
-- MAGIC
-- MAGIC ##### Considerações:
-- MAGIC - **Compatibilidade**: Talvez não haja suporte para views dinâmicas em todos os ambientes ou configurações do Databricks. Veja a documentação [Antes de começar](https://docs.databricks.com/aws/en/views/dynamic#before-you-begin)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Explorar Views usando o esquema de informação
-- MAGIC O **INFORMATION_SCHEMA** é um esquema baseado no padrão SQL, fornecido em todos os catálogos criados no Unity Catalog. Dentro do esquema de informações, você pode encontrar um conjunto de views que descrevem os objetos conhecidos pelo catálogo do esquema que você tem o privilégio de ver.
-- MAGIC
-- MAGIC
-- MAGIC Para esta demonstração, vamos explorar a **views** view dentro do **information_schema** em seu catálogo **dbacademy** para explorar as views que foram criadas.
-- MAGIC
-- MAGIC **NOTAS:**
-- MAGIC
-- MAGIC - [Esquema de informação](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-information-schema) documentação
-- MAGIC
-- MAGIC Documentação do [VIEWS](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/views) do Esquema de Informação

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Execute a célula abaixo para exibir o valor da variável SQL que foi criada para você durante o script de configuração da sala de aula que simplesmente armazena o nome do esquema `DA.schema_name` em uma variável SQL. Confirme se ele retorna seu nome **labuser** (exemplo, labuser1234_5678).

-- COMMAND ----------

values(DA.schema_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query a view **dbacademy.information_schema.views** e veja os resultados. 
-- MAGIC
-- MAGIC     Observe que:
-- MAGIC     - Exibe todas as views disponíveis para você, incluindo as views **information_schema**.
-- MAGIC     - A coluna **table_schema** contém o esquema da view.

-- COMMAND ----------

SELECT *
FROM dbacademy.information_schema.views;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Filtre a coluna **table_schema** usando a cláusula `WHERE` para filtrar todas as views dentro do esquema **labuser** usando a variável `DA.schema_name` que vimos anteriormente. Execute a query e visualize os resultados. Observe que:
-- MAGIC
-- MAGIC     - Você pode ver informações sobre as views que criamos na demonstração (**employees_vw**, **employees_mv**, **employees_mv_scheduled**, e **employees_dynamic_vw**).
-- MAGIC
-- MAGIC     - A coluna **view_definition** exibe a query que a view contém.
-- MAGIC
-- MAGIC     - A coluna **is_materialized** (a última coluna à direita) indica se a view está materializada.

-- COMMAND ----------

SELECT *
FROM dbacademy.information_schema.views
WHERE table_schema = DA.schema_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Recursos adicionais
-- MAGIC
-- MAGIC - [O que é uma view?](https://docs.databricks.com/aws/en/views/)
-- MAGIC
-- MAGIC - [Views Materializadas](https://www.databricks.com/glossary/materialized-views)
-- MAGIC
-- MAGIC - [Criar uma view dinâmica](https://docs.databricks.com/aws/en/views/dynamic)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
