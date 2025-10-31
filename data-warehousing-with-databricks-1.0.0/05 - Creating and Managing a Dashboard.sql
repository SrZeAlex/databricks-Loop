-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Criando e gerenciando um painel
-- MAGIC
-- MAGIC [Databricks IA/BI](https://www.databricks.com/product/ai-bi) é uma nova ferramenta de BI de autoatendimento que expande o perfil, os fluxos de trabalho e os casos de uso tradicionais de BI para atender às necessidades de usuários empresariais não técnicos. O objetivo é permitir que qualquer pessoa use dados para embasar decisões de negócios. Os usuários de destino não precisam saber SQL, como escrever código ou como configurar ou administrar computação ou armazenamento em nuvem. 
-- MAGIC
-- MAGIC Algumas coisas que diferenciam os painéis de IA/BI dos painéis DBSQL tradicionais são:
-- MAGIC * **Data model:** as queries não são objetos distintos em um painel de IA/BI. Na IA/BI, todo o conteúdo é empacotado dentro do próprio painel. Portanto, compartilhar um painel de IA/BI é tão simples quanto compartilhar o próprio painel.
-- MAGIC * **SQL-optional user experience:** IA/BI dá a capacidade de pesquisar e selecionar uma tabela de interesse para criar automaticamente um dataset com base nessa tabela. Agregações adicionais podem ser criadas sem SQL. No entanto, o SQL continua sendo uma opção de primeira classe para aqueles que preferem.
-- MAGIC * **Publish and share:** o estágio de design inicial de um painel começa no modo de rascunho. Quando estiver pronto para distribuição, ele pode ser publicado. As alterações no painel de rascunho não afetam a versão publicada. Os painéis publicados podem ser compartilhados com qualquer pessoa na organização, incluindo aqueles fora do Databricks workspace. 
-- MAGIC * **Visualization library and configuration:** Painéis de IA/BI definem a nova biblioteca de visualização e experiência de configuração.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pré-requisitos
-- MAGIC
-- MAGIC Neste notebook, construiremos painéis de IA/BI com base no dataset TPC-DI. Para acompanhar, você deve ter percorrido o processo de ingestão TPC-DI, conforme descrito no notebook *02.1 Ingestão de Dados*.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Anexando um dataset
-- MAGIC
-- MAGIC Para criar um painel, um dataset deve ser anexado, o que faremos nesta seção. Para este exemplo, usaremos a tabela *dimcustomer*.
-- MAGIC
-- MAGIC 1. No menu **New** na barra lateral esquerda, selecione **Dashboard** (abra em uma nova tab, para que você possa continuar acompanhando aqui). Isso leva você à view **Canvas** para definir o layout dos componentes do painel.
-- MAGIC 1. Antes de adicionar e configurar componentes, vamos associar um dataset selecionando a tab **Data**.!<br>
-- MAGIC    ![Data tab](images/dashboard/datatab.png)
-- MAGIC 1. Vamos usar uma tabela como dataset clicando em **Select a table**.<br>
-- MAGIC    ![Data tab](images/dashboard/selecttable.png)
-- MAGIC 1. Na caixa de diálogo **Select a table**, localize e selecione sua tabela *dimcustomer* e clique em **Confirm**.<br>
-- MAGIC    ![Data tab](images/dashboard/selectdimcustomer.png)
-- MAGIC 1. De volta à tab **Data**, vemos o dataset *dimcustomer* recém-adicionado. Com um SQL warehouse selecionado, execute a query default para exibir o conteúdo da tabela selecionada.<br>
-- MAGIC    ![Data tab](images/dashboard/runquery.png)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Adicionando visualizações
-- MAGIC
-- MAGIC Uma vez que um painel tem dados para visualizar, podemos começar o divertido trabalho de preenchê-lo com componentes que dão vida ao painel.
-- MAGIC
-- MAGIC 1. De volta à tab **Canvas**, localize e clique no botão **Add a visualization** próximo à parte inferior.<br>
-- MAGIC    ![Add a visualization](images/dashboard/addvisualization.png)
-- MAGIC 1. Selecione uma posição para sua visualização, que preencherá a tela com uma área para configurar a visualização.<br>
-- MAGIC     ![Add a visualization](images/dashboard/configurevisualization.png)
-- MAGIC 1. Use uma linguagem simples para descrever o que você gostaria de ver. Por exemplo: *Show me the distribution of customers based on gender.*<br>
-- MAGIC    ![Add a visualization](images/dashboard/withprompt.png)
-- MAGIC 1. Envie seu prompt e uma visualização será gerada em resposta. Neste ponto, você pode aceitar a visualização como está, rejeitá-la ou executar alguma configuração avançada usando os controles na barra lateral direita.<br>
-- MAGIC   ![Add a visualization](images/dashboard/piechart.png)
-- MAGIC    * Por exemplo, vamos alterar a transformação para agrupar por país em vez de gênero.<br>
-- MAGIC      ![Adjust transform](images/dashboard/adjusttransform.png)
-- MAGIC    * Como resultado, obtemos uma visualização correspondentemente ajustada.<br>
-- MAGIC      ![Group by country](images/dashboard/adjustedtransform.png)
-- MAGIC    * Antes de prosseguir, reverta a visualização para agrupar por gênero e clique em **Accept**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Filtragem
-- MAGIC
-- MAGIC Os painéis de IA/BI oferecem suporte à capacidade de filtrar dados no dataset, usando os seguintes tipos de filtro:
-- MAGIC * Dropdown de seleção única
-- MAGIC * Dropdown de seleção múltipla
-- MAGIC * Texto 
-- MAGIC * Dados 
-- MAGIC * Intervalo de datas
-- MAGIC
-- MAGIC Você pode conectar os filtros a um ou mais datasets. O filtro será então aplicado a todas as visualizações criadas no dataset selecionado. A seleção de filtros também é feita em cascata em todos os outros filtros. 
-- MAGIC
-- MAGIC 1. Localize e clique no botão **Add a filter** perto da parte inferior.<br>
-- MAGIC     ![Add a visualization](images/dashboard/addfilter.png)
-- MAGIC 1. Selecione uma posição para o filtro, que preencherá a tela com uma área para configurá-lo.<br>
-- MAGIC    ![Add a visualization](images/dashboard/positionfilter.png)
-- MAGIC 1. Na barra lateral direita, configure o filtro:
-- MAGIC    * Defina **Filter** como *Multiple values*.
-- MAGIC    * Adicione um campo e selecione *city*.<br>
-- MAGIC    ![Configure filter](images/dashboard/configurefilter.png)
-- MAGIC 1. Seu filtro agora pode ser usado. Por exemplo, se você selecionar *New York*, a visualização será atualizada com base na filtragem do dataset. Em outras palavras, você verá a distribuição de gênero para a cidade (ou cidades) selecionada.<br>
-- MAGIC    ![Configure filter](images/dashboard/newyorkgenders.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Colaborando
-- MAGIC
-- MAGIC Todos os novos painéis de IA/BI começam como um rascunho. Isso significa que qualquer trabalho que você fizer não afetará os painéis publicados (se houver). Você também pode compartilhar esses rascunhos com outras pessoas em seu espaço de trabalho e colaborar uns com os outros. 
-- MAGIC
-- MAGIC 1. Clique no botão **Share** no canto superior direito.<br>
-- MAGIC    ![Share button](images/dashboard/sharebutton.png)
-- MAGIC 1. Use a caixa de diálogo para configurar com quais usuários ou grupos você deseja colaborar. Você pode escolher as permissões a serem concedidas a cada entidade.<br>
-- MAGIC    ![Add a visualization](images/dashboard/configuresharing.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Publicação
-- MAGIC
-- MAGIC Os painéis de IA/BI podem ser publicados para disponibilizar uma cópia limpa do painel aos usuários dentro e fora do seu Databricks workspace. 
-- MAGIC
-- MAGIC 1. Clique no botão **Publish** no canto superior direito.<br>
-- MAGIC     ![Share button](images/dashboard/publishbutton.png)
-- MAGIC 1. A caixa de diálogo **Publish** é exibida. A maior escolha a fazer aqui é como lidar com as credenciais. Você tem duas opções:
-- MAGIC    * **Embed credentials (default):** Todos os visualizadores do painel executam queries usando suas credenciais e computação. Isso permitirá que os usuários vejam o painel mesmo que não tenham acesso aos dados ou ao SQL warehouse. No entanto, isso pode expor dados a usuários que não receberam acesso explícito a esses recursos. **This option must be selected if sharing with users outside of your Databricks workspace.**
-- MAGIC    * **Don’t embed credentials:** todos os visualizadores do painel executam queries usando suas próprias credenciais. Os visualizadores precisarão acessar o SQL warehouse anexado e os dados associados para exibir o painel. Esses requisitos implicam que essa opção só pode ser usada para usuários em seu Databricks workspace.<br>
-- MAGIC    ![Add a visualization](images/dashboard/publishdialog.png)
-- MAGIC 1. Com **Embed credential** selecionado, clique em **Publish**.
-- MAGIC 1. Você receberá um resumo do painel compartilhado, incluindo uma opção para copiar o link que você pode compartilhar com outras pessoas.<br>
-- MAGIC    ![Add a visualization](images/dashboard/shareddialog.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
-- MAGIC
