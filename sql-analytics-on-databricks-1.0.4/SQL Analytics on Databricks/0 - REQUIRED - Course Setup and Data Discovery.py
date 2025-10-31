# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 0 - OBRIGATÓRIO - Configuração do Curso e Descoberta de Dados
# MAGIC
# MAGIC Este notebook foi projetado para configurar o ambiente de laboratório para este curso e deve ser executado antes de prosseguir nas demonstrações e laboratórios. As próximas células deste notebook criarão os dados necessários para o curso.
# MAGIC
# MAGIC Depois que o ambiente de laboratório for configurado, você prosseguirá imediatamente para a primeira lição de demonstração, "Descoberta de Dados no Unity Catalog". Esta lição o guiará pela interface do usuário do Databricks para explorar o ambiente e os dados deste curso.
# MAGIC
# MAGIC ### Objetivos
# MAGIC - Localizar catálogos, esquemas, tabelas e visualizações no Catalog Explorer.
# MAGIC - Revise as permissões, configurações e metadados disponíveis no Catalog Explorer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## OBRIGATÓRIO - SELECIONE SERVERLESS COMPUTE
# MAGIC
# MAGIC Antes de executar células neste notebook, selecione a computação **Serverless** no canto superior direito, onde diz **Conectar**. O script de configuração do curso requer Python para configurar o ambiente do curso.
# MAGIC
# MAGIC Siga os passos para selecionar **Serverless**:
# MAGIC
# MAGIC 1. Navegue até o canto superior direito deste Notebook e clique no menu drop-down para selecionar a computação (o menu drop-down deve dizer **Conectar**).
# MAGIC
# MAGIC 2. Selecione **Serverless**.
# MAGIC
# MAGIC **NOTA:** Este é o único Notebook que requer a computação **Serverless**, pois precisamos do Python para configurar o ambiente. Os notebooks restantes usarão um SQL **shared_warehouse**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração da sala de aula
# MAGIC
# MAGIC Execute a célula a seguir para configurar o ambiente de trabalho para este notebook. Confirme se a mensagem *COURSE SETUP COMPLETE!* será exibida quando o programa for concluído.
# MAGIC
# MAGIC **ANOTAÇÕES:** 
# MAGIC - O objeto `DA` é usado apenas em cursos da Databricks Academy e não está disponível fora desses cursos. Ele fará referência dinâmica à informação necessária para executar o curso no ambiente de laboratório.
# MAGIC - OBRIGATÓRIO - Certifique-se de ter selecionado computação **Serverless** para esta célula.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP SCHEMA databrickstheloop.samantha_cruz CASCADE

# COMMAND ----------

# MAGIC %run ./Includes/0-Classroom-Setup-REQUIRED

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.1 Demo - descoberta de dados no Unity Catalog
# MAGIC ## A. Explore as pastas e arquivos do curso

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Conclua os passos a seguir para explorar as pastas e arquivos de curso disponíveis:
# MAGIC
# MAGIC    a. Na barra de navegação à esquerda, selecione o ícone de pasta diretamente à esquerda ![Folder Icon](./Includes/images/folder_icon.png). Aqui você verá uma variedade de pastas para o curso:
# MAGIC     - **M01 - Descobrimento de dados**
# MAGIC
# MAGIC     - **M02 - Importação de Dados**
# MAGIC
# MAGIC     - **M03 - Execução de SQL**
# MAGIC
# MAGIC     - **M04 - Análise de Query**
# MAGIC
# MAGIC     - **0 - OBRIGATÓRIO - Notebook de Configuração do Curso e Descobrimento de Dados**
# MAGIC     
# MAGIC     - Cada pasta corresponde a uma seção do curso e contém os notebooks necessários para as demonstrações e laboratórios.
# MAGIC
# MAGIC    b. Selecione a pasta **MO1 - Descobrimento de Dados**. Observe que essa pasta contém um notebook.
# MAGIC
# MAGIC    c. Use a seta <-- para navegar de volta para a pasta principal do curso **SQL Analytics no Databricks**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore os catálogos, esquemas e objetos de dados disponíveis

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Explorar objetos de dados usando o Painel de Navegação
# MAGIC 1. Conclua o procedimento a seguir para explorar os Catálogos e Objetos de Dados disponíveis usando o painel de navegação esquerdo:
# MAGIC
# MAGIC    a. No painel de navegação esquerdo, selecione o ícone Catálogo ![Catalog Icon](./Includes/images/catalog_icon.png). Observe que há vários grupos de catálogos:
# MAGIC
# MAGIC     - **Minha organização** - Catálogos disponíveis que você pode acessar e/ou visualizar.
# MAGIC
# MAGIC     - **Delta Shares Received** - Catálogos de Delta Sharing (somente leitura).
# MAGIC
# MAGIC     - **Legacy** - O legacy **hive_metastore**. Não usaremos isso no curso. O Databricks recomenda o uso do Unity Catalog para registrar e controlar todos os objetos de banco de dados, mas também fornece suporte de legacy ao Hive metastore para gerenciar esquemas, tabelas, views e funções.
# MAGIC
# MAGIC    b. Expanda o catálogo **dbacademy**. Aqui você verá **esquemas (ou bancos de dados)**.
# MAGIC
# MAGIC     - Você terá um esquema exclusivo chamado algo como: **labuser1234_5678**. Você usará esse esquema durante todo o curso.
# MAGIC
# MAGIC     - O **information_schema** é um esquema baseado no padrão SQL, fornecido em todos os catálogos criados no Unity Catalog. Dentro do esquema de informações, você pode encontrar um conjunto de views que descrevem os objetos conhecidos pelo catálogo do esquema que você tem o privilégio de ver. Você aprenderá mais sobre eles ao longo deste curso.
# MAGIC
# MAGIC    c. Expanda seu esquema **labuser**. Você deve ter dois objetos de dados:
# MAGIC
# MAGIC     - **Tabelas** - Tabelas são uma coleção de dados organizados por linhas e colunas. O objeto **Tabelas** também incluirá **views**.
# MAGIC
# MAGIC     - **Volumes** - Volumes são objetos do Unity Catalog que representam um volume lógico de armazenamento em um local de armazenamento de objetos na nuvem. Os volumes fornecem recursos para acessar, armazenar, controlar e organizar arquivos de dados não tabulares.
# MAGIC
# MAGIC    d. Expanda **Tabelas**. Observe que você tem acesso a uma variedade de tabelas neste laboratório.
# MAGIC
# MAGIC    e. Expandir **Volumes**. Observe que você tem um volume chamado **myfiles** neste laboratório. Lembre-se, os volumes armazenam arquivos não tabulares como `.txt`, `.parquet`, `.csv`, e muito mais.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Explore um catálogo usando o Catalog Explorer
# MAGIC 2. Em seguida, vamos usar a interface do Catalog Explorer para exibir nossos objetos de dados. Conclua as etapas a seguir para abrir o Catalog Explorer e exibir informações sobre o Catálogo:
# MAGIC
# MAGIC    a. No painel de navegação esquerdo, selecione o Catálogo **dbacademy**.
# MAGIC
# MAGIC    b. À direita, selecione as três reticências (ou o ícone do menu kebab) e escolha **Abrir no Catalog Explorer**. Isso abrirá o Catalog Explorer para o Catálogo **dbacademy** em uma nova tab do seu navegador.
# MAGIC
# MAGIC    c. Aqui, podemos ver informações sobre o Catálogo **dbacademy**. Observe que podemos ver três esquemas disponíveis no catálogo, semelhantes aos esquemas que vimos no painel de navegação esquerdo.
# MAGIC
# MAGIC    d. No Catalog Explorer, selecione a tab **Detalhes**. Aqui, você pode ver detalhes específicos sobre o catálogo.
# MAGIC
# MAGIC    e. Selecione a tab **Permissões**. Aqui, você pode ver as permissões disponíveis para o catálogo. Neste laboratório, todos os usuários da conta têm a capacidade de usar esse catálogo e ver apenas os objetos específicos que se aplicam a eles.
# MAGIC
# MAGIC    f. Deixe o Catalog Explorer aberto.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Explorar um esquema usando o Catalog Explorer
# MAGIC 3. Conclua o seguinte para usar o Catalog Explorer para exibir informações sobre seu esquema:
# MAGIC
# MAGIC    a. Navegue de volta para a tab **Visão geral** no Catalog Explorer.
# MAGIC
# MAGIC    b. Selecione seu esquema **labuser**. Agora você pode visualizar objetos de dados em seu esquema de laboratório.
# MAGIC
# MAGIC    c. Primeiro, você pode ver todas as tabelas disponíveis em seu esquema. Usando os botões, você pode ver **Tabelas**, **Volumes**, **Modelos**, e **Funções** se tiver esses objetos de dados.
# MAGIC
# MAGIC    d. Selecione a tab **Detalhes**. Observe que você pode ver informações sobre seu esquema. Vamos nos concentrar em uma visualização dos detalhes:
# MAGIC
# MAGIC     - Em **Proprietário**, você pode ver as informações do proprietário. Em nosso laboratório, uma conta de administrador é a proprietária do esquema desde que ele foi criado para você.
# MAGIC
# MAGIC     - Em **Metastore Id**, você pode ver o ID do metastore ao qual este catálogo está associado.
# MAGIC
# MAGIC     - Em **Tipo de catálogo**, você pode ver que este é um *MANAGED_CATALOG*. Isso significa que o local de armazenamento de catálogo desse esquema é gerenciado pelo Databricks.
# MAGIC
# MAGIC    e. Selecione a tab **Permissões**. Aqui você pode ver as permissões associadas a esse esquema. Em seu esquema, sua conta de usuário de laboratório tem *ALL PRIVILEGES* disponíveis no esquema.
# MAGIC
# MAGIC    f. Deixe o Catalog Explorer aberto.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B4. Explore uma tabela usando o Catalog Explorer
# MAGIC 4. Conclua o procedimento a seguir para usar o Gerenciador de Catálogos para exibir informações sobre uma tabela em seu esquema:
# MAGIC
# MAGIC    a. No Catalog Explorer, selecione a tab **Visão geral**. Você deve ver uma lista de tabelas em seu esquema de laboratório.
# MAGIC
# MAGIC    b. Selecione a tabela **aus_opportunities**. Aqui, você pode ver informações sobre a tabela.
# MAGIC
# MAGIC    c. Na tab **Visão geral**, você pode ver uma visão geral da sua tabela. Observe o seguinte:
# MAGIC
# MAGIC     - A Databricks Data Intelligence Platform fornece sugestões de IA que você pode usar para fornecer um resumo de sua tabela e colunas.
# MAGIC
# MAGIC     - Você pode ver uma lista de todas as colunas em sua tabela.
# MAGIC
# MAGIC     - No lado direito abaixo de **Sobre esta tabela**, você pode ver informações detalhadas sobre a tabela, como o **Proprietário**, **Fonte de dados**, e **Tamanho**.
# MAGIC
# MAGIC     - abaixo da seção **Sobre esta tabela**, você pode adicionar **Tags** e um **Filtro de linha** à tabela.
# MAGIC
# MAGIC    d. Vamos adicionar a **Descrição Sugerida da IA** à tabela selecionando **Aceitar**.
# MAGIC
# MAGIC    e. Vamos adicionar comentários gerados por IA às colunas selecionando **AI Generate**. Uma lista de **Comentários** deve aparecer. Basta selecionar a marca de seleção para a primeira coluna, **opportunityid**.
# MAGIC
# MAGIC    f. Selecione a tab **Dados de Amostra** para exibir uma amostra dos dados.
# MAGIC
# MAGIC    g. Selecione a tab **Detalhes** para exibir informações detalhadas sobre a tabela. Observe o seguinte:
# MAGIC
# MAGIC     - Esta tabela é uma tabela *MANAGED*.
# MAGIC
# MAGIC     - Você pode ver o local de armazenamento em nuvem da tabela.
# MAGIC
# MAGIC     - Você pode ver as propriedades específicas da tabela Delta.
# MAGIC
# MAGIC     - Você pode ver quem criou a tabela.
# MAGIC
# MAGIC    h. Selecione a tab **Permissões** para ver as permissões da tabela. Neste exemplo, você criou a tabela, portanto, tem *ALL PRIVILEGES* nesta tabela.
# MAGIC
# MAGIC    i. Selecione **Grant**. Aqui, você pode conceder a outras pessoas permissões específicas para usar essa tabela. Selecione **Cancelar**.
# MAGIC
# MAGIC    j. Selecione a guia **Histórico**. Aqui, você pode ver o histórico de alterações nesta tabela. Neste exemplo, você deve ver três versões:
# MAGIC
# MAGIC     - A criação inicial da tabela.
# MAGIC
# MAGIC     - A modificação do resumo que completamos com IA.
# MAGIC
# MAGIC     - Os comentários da coluna que adicionamos com IA.
# MAGIC
# MAGIC    k. Selecione a tab **Linhagem** para exibir a linhagem da tabela. Observe que o **dbacademy_aus_sales.v01.opportunities** é mostrado. Isso ocorre porque o script de instalação usou essa tabela para criar uma tabela em nosso esquema.
# MAGIC
# MAGIC    l. Selecione **Ver gráfico de linhagem** na extrema direita. Isso exibirá uma representação visual da linhagem da tabela. Novamente, nossa tabela foi criada usando a tabela **dbacademy_aus_sales.v01.opportunities**. Feche o gráfico.
# MAGIC
# MAGIC    m. Selecione a tab **Percepções** para visualizar as queries recentes mais frequentes e os usuários de qualquer tabela registrada no Unity Catalog. A tab Percepções relata queries frequentes e acesso de usuários nos últimos 30 dias. Neste exemplo, a tabela é nova e não há percepções.
# MAGIC
# MAGIC    n. Selecione a tab **Qualidade** para criar um monitor para rastrear alterações na qualidade dos dados e na distribuição estatística. Nesta demonstração, não criaremos um monitor, mas a opção está lá.
# MAGIC
# MAGIC    o. Feche o Catalog Explorer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Explore o Databricks Marketplace

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Conclua o seguinte para explorar o Databricks Marketplace:
# MAGIC
# MAGIC    a. No painel mais à esquerda, clique com o botão direito do rato em **Marketplace** e selecione *Abrir em uma nova tab*.
# MAGIC
# MAGIC    b. Selecione o ícone de filtro à esquerda da barra de pesquisa ![Filter Marketplace Icon](./Includes/images/marketplace_filter_icon.png).
# MAGIC
# MAGIC    c. Em **Provedores**, localize *Databricks*.
# MAGIC
# MAGIC    d. Aqui estão os objetos de dados que o Databricks compartilha por meio do Marketplace.
# MAGIC
# MAGIC    e. Role até a parte inferior da página e selecione **Mostrar mais** até que a opção desapareça.
# MAGIC
# MAGIC    f. Pesquise o compartilhamento *Dados simulados de vendas e oportunidades na Austrália* e selecione-o.
# MAGIC
# MAGIC    g. Esse é um dos compartilhamentos que é utilizado neste curso.
# MAGIC
# MAGIC    h. No canto superior direito do compartilhamento, selecione **Abrir**. Isso abrirá esse compartilhamento do Marketplace, que já configuramos neste laboratório no Catalog Explorer. O nome do catálogo para o compartilhamento é **dbacademy_aus_sales**. 
# MAGIC
# MAGIC    **NOTA:** Se você não tiver esse catálogo compartilhado, poderá acessá-lo selecionando **Solicitar acesso** onde **Abrir** estava localizado.
# MAGIC
# MAGIC    i. Deixe o Catalog Explorer aberto.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Vamos explorar o catálogo de Compartilhamento do Marketplace que está configurado em nosso laboratório no Catalog Explorer.
# MAGIC
# MAGIC    um. No Gerenciador de Catálogos que você abriu na etapa anterior, você pode estar em uma tabela dentro do catálogo do Marketplace. Na parte superior, selecione o nome do catálogo **dbacademy_aus_sales**.
# MAGIC
# MAGIC    b. Na barra de navegação esquerda do Catalog Explorer, você pode ver na seção **Delta Shares Received** que estamos explorando o catálogo compartilhado do Databricks Marketplace, que foi configurado para você, chamado **dbacademy_aus_sales**.
# MAGIC
# MAGIC    c. No Catalog Explorer, no lado direito em **Sobre este catálogo**, você pode ver que o **Tipo** é *Delta Sharing*.
# MAGIC
# MAGIC    d. Selecione a tab **Permissões**. Observe que você tem várias permissões neste catálogo; no entanto, você não pode gravar nesse catálogo porque ele é um catálogo compartilhado.
# MAGIC
# MAGIC    e. Feche o Catalog Explorer.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
