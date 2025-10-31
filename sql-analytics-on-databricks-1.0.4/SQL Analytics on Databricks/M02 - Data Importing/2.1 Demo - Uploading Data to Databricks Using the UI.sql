-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2.1 Demo - Upload de dados para o Databricks usando a interface do usuário
-- MAGIC
-- MAGIC Nesta demonstração, faremos upload de um pequeno arquivo CSV em um volume dentro do Databricks usando a interface do usuário. Além disso, você também aprenderá a usar a interface do usuário de upload de arquivos do Databricks para criar uma tabela gerenciada do Databricks.
-- MAGIC
-- MAGIC ### Objetivos
-- MAGIC - Demonstrar como fazer upload de um arquivo CSV em um volume no Databricks.
-- MAGIC - Demonstrar como fazer upload de um arquivo CSV para criar uma tabela gerenciada do Databricks.
-- MAGIC
-- MAGIC **NOTA:** Dependendo das restrições do seu computador, talvez você não consiga concluir a demonstração a seguir, que inclui baixar um arquivo CSV localmente e, em seguida, usar o navegador para carregar o arquivo de volta no Databricks.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OBRIGATÓRIO - SELECIONE UM SHARED SQL WAREHOUSE
-- MAGIC
-- MAGIC Antes de executar células neste Notebook, selecione o **SHARED SQL WAREHOUSE** no laboratório. Siga estes passos:
-- MAGIC
-- MAGIC 1. Navegue até o canto superior direito deste Notebook e clique na lista dropdown para selecionar compute (pode dizer **Conectar**). Preencha um dos itens a seguir abaixo:
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
-- MAGIC Lembre-se de que a configuração do laboratório é criada com o [0 - OBRIGATÓRIO - Configuração do curso e descobrimento de dados]($../0 - REQUIRED - Course Setup and Data Discovery). Se você encerrar a sessão de laboratório ou se a sessão atingir o limite de tempo, seu ambiente será redefinido e você precisará executar novamente a Configuração do curso do Notebook.

-- COMMAND ----------

-- MAGIC %run ../Includes/2.1-Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Faça upload de um arquivo CSV local em um volume usando a interface do usuário
-- MAGIC
-- MAGIC Nesta seção, demonstraremos como fazer upload de um arquivo CSV local para um volume do Databricks. Para replicar esse processo, fornecemos o arquivo CSV na pasta **dados** para você download no computador local.
-- MAGIC
-- MAGIC **Nota:** Dependendo das políticas de segurança do seu computador portátil, talvez você não consiga download e fazer upload de um arquivo no Databricks.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Conclua os seguintes passos para baixar o arquivo CSV localmente:
-- MAGIC
-- MAGIC    a. Na pasta **M02 - Importação de Dados**, há uma pasta **dados**. Abra a pasta **dados** e você verá um arquivo chamado **au_products.csv**.
-- MAGIC
-- MAGIC    b. Passe o rato sobre o arquivo **au_products.csv** e, em seguida, selecione as três reticências à direita do arquivo e escolha **Download**. Isso fará o download do arquivo CSV para a pasta de download do seu computador.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Conclua os passos a seguir para fazer upload do arquivo CSV em seu volume:
-- MAGIC
-- MAGIC    a. No painel mais à esquerda, clique com o botão direito do rato em **Catálogo** e selecione *Abrir em Nova Tab*.
-- MAGIC
-- MAGIC    b. No Catalog Explorer, selecione o catálogo **dbacademy**.
-- MAGIC
-- MAGIC    c. Selecione seu esquema **labuser**.
-- MAGIC
-- MAGIC    d. Selecione **Volumes**, em seguida, escolha o volume **myfiles**.
-- MAGIC
-- MAGIC    e. No canto superior direito, selecione **Upload para este volume**.
-- MAGIC
-- MAGIC    f. Carregue o arquivo **au_products.csv** que você baixou na etapa anterior.
-- MAGIC
-- MAGIC    g. Selecione **Upload** para fazer upload do arquivo CSV para o volume do Databricks.
-- MAGIC
-- MAGIC    h. Feche o Catalog Explorer.
-- MAGIC
-- MAGIC    **Nota:** Se você tiver as permissões necessárias em seu computador portátil para upload de um arquivo, o arquivo será carregado para o volume **myfiles**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Conclua as etapas a seguir para exibir o arquivo CSV no volume do Databricks:
-- MAGIC
-- MAGIC    a. No painel de navegação esquerdo, selecione o ícone de catálogo ![Ícone do catálogo](../Includes/images/catalog_icon.png).
-- MAGIC
-- MAGIC    b. Expanda o catálogo **dbacademy**.
-- MAGIC
-- MAGIC    c. Expanda seu esquema **labuser**.
-- MAGIC
-- MAGIC    d. Expanda **Volumes** e seu volume **myfiles**. Você verá que o arquivo **au_products.csv** foi carregado no volume do Databricks (talvez seja necessário fazer refresh do volume usando o botão refresh acima da barra de pesquisa do catálogo).
-- MAGIC
-- MAGIC    **Nota:** Lembre-se, os volumes do Databricks são espaços de armazenamento lógicos usados para armazenar e acessar arquivos. Eles fazem parte do Unity Catalog, que é usado para governar datasets não tabulares, como arquivos CSV.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Com o arquivo CSV em um volume, agora você pode exibir e criar programaticamente uma tabela a partir do arquivo CSV. Mostraremos como fazer isso na próxima demonstração.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Carregue um arquivo CSV local como uma tabela usando a interface do usuário
-- MAGIC Nesta seção, carregaremos o mesmo arquivo **au_products.csv** no Databricks, mas desta vez criaremos uma tabela usando a interface do usuário em vez de carregar o arquivo CSV diretamente em um volume.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Conclua os passos a seguir para carregar o arquivo CSV como uma tabela em seu esquema:
-- MAGIC
-- MAGIC    a. No painel mais à esquerda, clique com o botão direito do rato em **Catálogo** e selecione *Abrir em Nova Tab*.
-- MAGIC
-- MAGIC    b. No Catalog Explorer, selecione o catálogo **dbacademy**.
-- MAGIC
-- MAGIC    c. Selecione seu esquema **labuser**.
-- MAGIC
-- MAGIC    d. No canto superior direito, selecione o dropdown **Criar**.
-- MAGIC
-- MAGIC    e. Selecione **Table**.
-- MAGIC    
-- MAGIC    f. Adicione seu arquivo baixado localmente, **au_products.csv**, à interface do usuário.
-- MAGIC
-- MAGIC    g. Observe que o **Criar ou modificar tabela a partir do upload de arquivos** fornece uma variedade de opções a serem definidas ao importar seu arquivo CSV como uma tabela.
-- MAGIC
-- MAGIC    h. Altere o nome da tabela para **aus_products_ui**.
-- MAGIC
-- MAGIC    i. Como este é um arquivo CSV simples, deixe as opções restantes como estão e selecione **Criar tabela** no canto inferior direito. Isso criará a tabela no Databricks a partir do arquivo CSV local.
-- MAGIC
-- MAGIC    j. Depois que a tabela for criada, aceite a **Descrição Sugerida de IA**.
-- MAGIC
-- MAGIC    **Nota:** Se você tiver as permissões necessárias em seu computador portátil para carregar um arquivo, o arquivo será carregado como uma nova tabela chamada **aus_products_ui** para o Databricks. No entanto, suas políticas de segurança podem proibir o upload de um arquivo.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. No painel de navegação esquerdo, refresh seu esquema **labuser**. A nova tabela **aus_products_ui** deve aparecer no seu esquema.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
