-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Preparando TPC-DI

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Introdução
-- MAGIC
-- MAGIC [Databricks TPC-DI](https://github.com/shannon-barrow/databricks-tpc-di) é uma implementação de especificações derivadas do Benchmark [TPC-DI](http://tpc.org/tpcdi/default5.asp). Além de facilitar uma arquitetura de dados adequada para benchmarking padronizado, esse código demonstra a mecânica de um aplicativo ETL típico de integração de dados. O código usa o software fornecido pelo projeto TPC para fazer isso. [Siga este link](https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY) para obter mais informações ou se você mesmo quiser acessar o software TPC.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Opções default
-- MAGIC
-- MAGIC Aqueles familiarizados com [Databricks TPC-DI](https://github.com/shannon-barrow/databricks-tpc-di) saberão que o processo é bastante configurável. Este notebook fornece uma derivação deste trabalho que está ajustada com as necessidades específicas deste ambiente de treinamento. Isso nos permite usar um dataset seguindo as especificações TPC-DI para demonstrar uma variedade de tarefas de data warehousing usando a Databricks Data Intelligence Platform.
-- MAGIC
-- MAGIC Especificamente, as configurações de parâmetros significativos são as seguintes:
-- MAGIC - **Scale Factor**: Isso se correlaciona com a quantidade de dados que serão processados. Nem o número total de arquivos e tabelas processados nem o DAG de fluxo de trabalho são afetados, embora a quantidade de dados por arquivo seja. Nessa configuração, um Scale Factor de 10 é usado, que se alinha a aproximadamente 1 GB de dados brutos.
-- MAGIC - **Workflow Type**: Controla o tipo de computação subjacente. Nessa configuração, aproveitamos um SQL warehouse pré-configurado para fazer uma execução incremental (benchmarked) ou executar todos os lotes em uma única passagem.

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
-- MAGIC 1. Quando o cluster estiver em execução, execute a célula a seguir para preparar o ambiente de execução para o restante do processo.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Geração de dados
-- MAGIC
-- MAGIC A geração de dados envolve a criação de dados brutos em vários formatos, conforme exigido pela especificação TPC. Os dados brutos são gerados por um componente de software fornecido pela TPC. As etapas na geração de dados são as seguintes:
-- MAGIC * Copie o artefato fornecido pelo TPC (`DIGen.jar`) e suas dependências para o ambiente de execução do nó do driver (estes são armazenados na subpasta *./tools/datagen*).
-- MAGIC * Execute um script wrapper python que invoca o gerador de dados, que irá gerar arquivos de dados no sistema de arquivos local do nó do driver (na pasta */localdisk0/tmp/tpcdi*).
-- MAGIC * Copie os arquivos gerados em um volume. O volume de destino está na saída da célula anterior.
-- MAGIC
-- MAGIC Por brevidade, os dados brutos são pré-gerados nesse ambiente de laboratório, portanto, a ferramenta ignorará essa etapa, pois percebe que os dados já foram gerados.

-- COMMAND ----------

-- MAGIC %run ./tools/data_generator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preparativos para a ingestão de dados
-- MAGIC
-- MAGIC Com os dados brutos de entrada prontos para ingestão, agora criaremos um Databricks workflow que, quando acionado, criará e carregará uma coleção de tabelas que implementa as regras e os requisitos de negócios conforme estabelecido na especificação TPC.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criar um fluxo de trabalho
-- MAGIC Execute a célula a seguir para criar um Databricks workflow que ingerirá os dados brutos da seção anterior. Assim que concluir, ele exibirá um link permitindo aceder ao fluxo de trabalho.

-- COMMAND ----------

-- MAGIC %run ./tools/generate_workflow

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Abra o fluxo de trabalho
-- MAGIC
-- MAGIC Abra o fluxo de trabalho seguindo um destes procedimentos:
-- MAGIC * Siga o link fornecido na saída da célula anterior (abre em uma nova tab), OU
-- MAGIC * Clique no item **Workflows** na barra lateral esquerda (abra-o em uma nova tab para que possa continuar a seguir estas instruções) e localize o fluxo de trabalho com o nome específico.
-- MAGIC
-- MAGIC Vamos explorar os detalhes do fluxo de trabalho no próximo notebook.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
-- MAGIC
