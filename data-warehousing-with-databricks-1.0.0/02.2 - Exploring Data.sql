-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Explorando dados

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pré-requisitos
-- MAGIC
-- MAGIC Neste notebook, exploraremos os resultados de um processo de ETL que criou um dataset em conformidade com as convenções estabelecidas na especificação [TPC-DI](http://tpc.org/tpcdi/default5.asp). Para executar esta demonstração, você deve ter executado a ingestão conforme descrito no notebook *02.1 Ingestão de Dados*.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuração
-- MAGIC
-- MAGIC Antes de começar, vamos realizar algumas configurações preliminares.
-- MAGIC
-- MAGIC 1. Este curso requer um cluster multifuncional dedicado que foi criado automaticamente para você. Anexe-o agora clicando no menu de computação no canto superior direito da página.<br>
-- MAGIC ![](images/preparingtpcdi/compute_menu.png)
-- MAGIC 1. Na caixa de diálogo de computação:
-- MAGIC    * Certifique-se de que **General compute** esteja selecionada.
-- MAGIC    * No menu dropdown, selecione o cluster dedicado (ele ainda pode estar iniciando).
-- MAGIC    * Clique em **Start and attach**.<br>
-- MAGIC ![](images/preparingtpcdi/compute_dialog.png)
-- MAGIC 1. Quando o cluster estiver em execução, execute a célula a seguir para preparar o ambiente de execução para o restante da demonstração.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploração através de SQL
-- MAGIC
-- MAGIC Vamos primeiro explorar nosso dataset TPC-DI por meio de DBSQL, usando uma série de instruções SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Esquemas
-- MAGIC
-- MAGIC Vamos ver os esquemas (bancos de dados) criados pelo processo de preparação e ingestão do TPC-DI usando a instrução `SHOW SCHEMAS`. Observe que isso opera no catálogo selecionado no momento, que, neste ambiente de treinamento, é definido automaticamente para seu catálogo pessoal.

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O fluxo de trabalho criou dois esquemas:
-- MAGIC **tpcdi_dbsql_10*: contém todas as tabelas de data mart TPC-DI para consumo downstream
-- MAGIC * *tpcdi_dbsql_10_stage*: Contém tabelas de preparo (tabelas intermediárias usadas para criar as tabelas data mart)
-- MAGIC
-- MAGIC Observe que o componente *dbsql* dos nomes vem da seleção de um SQL warehouse como o recurso de computação que executa o trabalho, e a parte *10* é derivada do scale factor pré-selecionado.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tabelas
-- MAGIC
-- MAGIC Vamos explorar as tabelas e como algumas delas são configuradas. Usaremos a tabela *dimcustomer* como exemplo, mas sinta-se à vontade para explorar outras também.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Examinando metadados e informações da tabela
-- MAGIC
-- MAGIC A instrução `DESCRIBE` pode ser usada para obter informações básicas sobre o esquema da tabela.

-- COMMAND ----------

DESCRIBE tpcdi_dbsql_10.dimcustomer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Como podemos ver, a tabela contém uma chave substituta *sk_customerid*. Uma key substituta é usada para identificar exclusivamente uma linha, mas não ocorre naturalmente nos dados da linha e precisa ser derivada ou calculada artificialmente de alguma forma. Não tem sentido do ponto de vista empresarial; ele é usado para fins de gerenciamento de banco de dados.
-- MAGIC
-- MAGIC Informações adicionais sobre como a tabela é gerenciada podem ser obtidas adicionando a instrução `EXTENDED` modificador à `DESCRIBE`.

-- COMMAND ----------

DESCRIBE EXTENDED tpcdi_dbsql_10.dimcustomer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Informações adicionais da tabela e estatísticas podem ser obtidas usando o modificador `DETAIL`.

-- COMMAND ----------

DESCRIBE DETAIL tpcdi_dbsql_10.dimcustomer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Amostragem de dados da tabela
-- MAGIC
-- MAGIC Podemos examinar os dados da tabela usando uma instrução simples `SELECT`. Nesse caso, limitamos a query a 100 linhas para mantê-la curta.

-- COMMAND ----------

SELECT * FROM tpcdi_dbsql_10.dimcustomer LIMIT 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Histórico da tabela
-- MAGIC
-- MAGIC Entender como uma tabela mudou ao longo do tempo pode ser um aspecto importante do gerenciamento de um data warehouse. Mais uma vez, a instrução `DESCRIBE` com o modificador `HISTORY` pode ser usada para obter o histórico da tabela.

-- COMMAND ----------

DESCRIBE HISTORY tpcdi_dbsql_10.dimcustomer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O uso `VERSION AS OF` como parte de suas queries permite que você segmente versões específicas do registro histórico de uma tabela.

-- COMMAND ----------

SELECT COUNT(*) FROM tpcdi_dbsql_10.dimcustomer VERSION AS OF 0

-- COMMAND ----------

SELECT COUNT(*) FROM tpcdi_dbsql_10.dimcustomer VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Governança de dados
-- MAGIC
-- MAGIC Vamos explorar as permissões e como algumas delas são configuradas. Usaremos a tabela *dimcustomer* como exemplo, mas sinta-se à vontade para explorar outras também.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Explorando permissões
-- MAGIC
-- MAGIC Vamos observar as permissões em vários níveis da hierarquia de dados, começando com a tabela *dimcustomer*. Para isso usamos a instrução `SHOW GRANTS`.

-- COMMAND ----------

SHOW GRANTS ON tpcdi_dbsql_10.dimcustomer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos examinar as permissões no esquema que contém, *tpcdi_sbsql_10*.

-- COMMAND ----------

SHOW GRANTS ON SCHEMA tpcdi_dbsql_10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Da maneira como o ambiente de aprendizagem é configurado, ambas as respostas são idênticas, já que **ALL_PRIVILEGES** é concedida no nível do catálogo, e essas permissões são herdadas para baixo; essa é uma propriedade importante do modelo de segurança do Unity Catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Granting permissions
-- MAGIC
-- MAGIC A concessão de permissões permite que outras pessoas acessem um objeto de uma maneira específica e, no SQL, é feita por meio da instrução `GRANT`.
-- MAGIC
-- MAGIC Observe que as permissões em um objeto só podem ser gerenciadas por:
-- MAGIC * Um administrador de metastore
-- MAGIC * O proprietário do objeto
-- MAGIC * Alguém que tem o privilégio `MANAGE` sobre o objeto
-- MAGIC * O proprietário do catálogo ou esquema que contém o objeto

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Privilégios explícitos
-- MAGIC O Unity Catalog oferece suporte a dois métodos para concessão de privilégios: *explícito* e *herdado*.
-- MAGIC
-- MAGIC Com a atribuição explícita de privilégios, você atribui os privilégios diretamente nos objetos aos quais eles se aplicam. Suponhamos, por exemplo, que quiséssemos conceder `SELECT` em duas tabelas. Seguindo um esquema de privilégios explícito, precisaríamos conceder os seguintes privilégios ao beneficiário:
-- MAGIC * `SELECT` nas tabelas
-- MAGIC * `USE SCHEMA` no esquema que contém
-- MAGIC * `USE CATALOG` no catálogo contendo
-- MAGIC
-- MAGIC Pode dar muito trabalho gerenciar todas essas permissões, mas há pouco espaço para erros, já que estamos nomeando todos os objetos acessíveis explicitamente. Em outras palavras, há muito pouca chance de conceder permissões em excesso ao seguir essa abordagem.
-- MAGIC
-- MAGIC Veja como abriríamos a tabela *dimcustomer* para todos verem:

-- COMMAND ----------

GRANT SELECT ON tpcdi_dbsql_10.dimcustomer TO `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC No modelo de segurança do Unity Catalog, porém, isso não é suficiente. O beneficiário precisa de permissões adicionais nos níveis de esquema e catálogo.

-- COMMAND ----------

GRANT USE_SCHEMA ON SCHEMA tpcdi_dbsql_10 TO `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Finalmente, precisaríamos de uma concessão semelhante à seguinte, no entanto, isso não funcionará no ambiente de aprendizagem devido à maneira segura como ele é configurado.
-- MAGIC ```
-- MAGIC GRANT USE_CATALOG ON CATALOG <catalog> TO `account users`
-- MAGIC ```
-- MAGIC Antes de continuar, vamos verificar novamente as permissões `dimcustomer` na tabela:

-- COMMAND ----------

SHOW GRANTS ON tpcdi_dbsql_10.dimcustomer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Como antes, você tem permissões `ALL_PRIVILEGES` através do catálogo que contém, mas agora também vemos que todos os usuários têm permissões `SELECT` nele também.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Privilégios herdados
-- MAGIC Como mencionado anteriormente, o Unity Catalog oferece suporte a dois métodos para conceder privilégios: *explícito* e *herdado*, e acabamos de ver como o modelo explícito funciona.
-- MAGIC
-- MAGIC Por ser tão explícito, é uma maneira bastante segura de administrar permissões e não muito propensa a erros ou concessão excessiva de permissões. No entanto, não é muito conveniente. Suponhamos que quiséssemos conceder `SELECT` em todas as tabelas.
-- MAGIC
-- MAGIC É aqui que os privilégios herdados podem facilitar a vida. Em vez de conceder `SELECT` explicitamente em todas as tabelas, podemos concedê-lo no nível do esquema:

-- COMMAND ----------

GRANT SELECT ON SCHEMA tpcdi_dbsql_10 TO `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Mais uma vez, vamos revisar as permissões na tabela `dimcustomer` :

-- COMMAND ----------

SHOW GRANTS ON tpcdi_dbsql_10.dimcustomer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que todos os usuários ainda têm `SELECT` diretamente na própria tabela como um remanescente da seção anterior. Mas observe também que agora ele também é concedido por meio do esquema de contenção. Vamos verificar uma das outras tabelas de dimensão:

-- COMMAND ----------

SHOW GRANTS ON tpcdi_dbsql_10.dimaccount

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe como todos os usuários também têm `SELECT`, herdado do esquema contido. Não só se `SELECT` aplica a todas as tabelas no esquema agora, mas também a quaisquer tabelas *futuras* criadas nesse esquema.
-- MAGIC
-- MAGIC A herança de privilégios facilita a execução de concessões em massa no Unity Catalog para simplificar a administração de permissões; mas você deve estar sempre ciente de que não concede permissões em excesso usando essa abordagem. Seguir as regras fundamentais do menor privilégio ajudará aqui.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Revogando permissões
-- MAGIC
-- MAGIC Nenhum modelo de segurança estaria completo sem a capacidade de revogar permissões. No SQL, isso é feito por meio da instrução `REVOKE`.
-- MAGIC
-- MAGIC Como exemplo, vamos reverter a `SELECT` concessão no `tpcdi_dbsql_10` esquema.

-- COMMAND ----------

REVOKE SELECT ON SCHEMA tpcdi_dbsql_10 FROM `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos verificar novamente as permissões na tabela *dimaccount*:

-- COMMAND ----------

SHOW GRANTS ON tpcdi_dbsql_10.dimaccount

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ninguém mais poderia ler essa tabela agora, porque a concessão herdada `SELECT` foi revogada. Voltamos efetivamente agora ao modelo de privilégio explícito.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Catalog Explorer
-- MAGIC
-- MAGIC O Catalog Explorer é um componente poderoso da Databricks Data Intelligence Platform, permitindo que você navegue e gerencie interativamente seus ativos de dados de um só lugar.
-- MAGIC
-- MAGIC 1. Abra o [Catalog Explorer](/explore/data) e selecione seu catálogo pessoal (como saída na célula de configuração).
-- MAGIC 1. Selecione o esquema *tpcdi_dbsql_10*.
-- MAGIC 1. Selecione uma tabela. Para este exemplo, usamos a tabela *dimcustomer*.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Examinando metadados e informações da tabela
-- MAGIC
-- MAGIC A tab **Overview** mostra informações sobre a tabela:
-- MAGIC * Resumo do esquema da tabela
-- MAGIC * Informações administrativas como tamanho e número de arquivos
-- MAGIC * Hora da última atualização, dando um indicador de quão frescos os dados estão
-- MAGIC
-- MAGIC Metadados de tabela, como tags e comentários, aprimoram muito seu catálogo de dados e o tornam mais fácil de gerenciar. Para ajudar nesse sentido, o Databricks fornece comentários sugeridos por IA para ajudar os usuários a entender os dados.
-- MAGIC
-- MAGIC ![Overview tab](images/exploringdata/overview.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Amostragem de dados da tabela
-- MAGIC
-- MAGIC A tab **Sample Data** fornece uma olhada nos dados da tabela. Essa view pode ser filtrada e classificada, se necessário.
-- MAGIC
-- MAGIC ![Sample Data tab](images/exploringdata/sampledata.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Governança de dados
-- MAGIC
-- MAGIC A tab **Permissions** fornece uma maneira fácil de gerenciar o controle de acesso de tabelas e todos os ativos de dados em seu metastore, equivalente ao uso de instruções `GRANT` e `REVOKE` em SQL.
-- MAGIC
-- MAGIC ![Permissions tab](images/exploringdata/permissions.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Histórico da tabela
-- MAGIC
-- MAGIC Entender como uma tabela mudou ao longo do tempo pode ser um aspecto importante do gerenciamento de um data warehouse. A tab **History** permite que você veja quais operações afetaram uma tabela ao longo do tempo.
-- MAGIC
-- MAGIC ![History tab](images/exploringdata/history.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Linhagem
-- MAGIC
-- MAGIC A tab **Lineage** fornece uma view da linhagem da tabela. A linhagem de dados captura e visualiza quase em tempo real a linhagem de seus dados à medida que eles se movem e se transformam em diferentes objetos, como tabelas, notebooks, painéis e muito mais. A linhagem está disponível até o nível da coluna. Isso permite que você obtenha percepções sobre as origens de seus dados, bem como os possíveis impactos downstream de alterações em seus pipelines.
-- MAGIC
-- MAGIC ![Lineage tab](images/exploringdata/lineage.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
-- MAGIC
