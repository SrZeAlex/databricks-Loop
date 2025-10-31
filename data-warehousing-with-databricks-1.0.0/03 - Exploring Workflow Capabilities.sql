-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Explorando os recursos do fluxo de trabalho

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pré-requisitos
-- MAGIC
-- MAGIC Neste notebook, exploraremos a estrutura do fluxo de trabalho implementando um processo típico de ETL que ingerirá um dataset que está em conformidade com as convenções estabelecidas na especificação [TPC-DI](http://tpc.org/tpcdi/default5.asp). Para executar esta demonstração, você deve ter executado os preparativos descritos no notebook *01 Preparando TPC-DI*.
-- MAGIC
-- MAGIC Você também precisará se conectar ao cluster multifuncional dedicado para executar o código Python posteriormente neste notebook.
-- MAGIC
-- MAGIC 1. Conecte-se ao cluster agora clicando no menu de computação no canto superior direito da página.<br>
-- MAGIC ![](images/preparingtpcdi/compute_menu.png)
-- MAGIC 1. Na caixa de diálogo de computação:
-- MAGIC    * Certifique-se de que **General compute** esteja selecionada.
-- MAGIC    * No menu dropdown, selecione o cluster dedicado (ele ainda pode estar iniciando).
-- MAGIC    * Clique em **Start and attach**.<br>
-- MAGIC ![](images/preparingtpcdi/compute_dialog.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explorando o fluxo de trabalho
-- MAGIC
-- MAGIC Abra o fluxo de trabalho criado no notebook *01 Preparando TPC-DI* seguindo um destes procedimentos:
-- MAGIC 1. Siga o link fornecido na saída desse notebook.
-- MAGIC 1. Clique no item **Workflows** na barra lateral esquerda (abra-o em uma nova tab para que você possa continuar a seguir estas instruções).
-- MAGIC
-- MAGIC Abra a tab **Tasks** ou o fluxo de trabalho. Lá, você verá um DAG mostrando todas as tarefas para o fluxo de trabalho, sinta-se à vontade para clicar em tarefas individuais para explorar as configurações da tarefa específica.![](path)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tarefas SQL
-- MAGIC
-- MAGIC Vamos selecionar **ingest_ProspectIncremental**. Esta é uma tarefa do tipo **Notebook**. A Databricks oferece suporte a vários tipos de tarefas e esse tipo simplesmente executa um notebook.
-- MAGIC
-- MAGIC Chame sua atenção para o campo **Path**, que especifica o local do notebook a ser executado. Se seguirmos isso, navegaremos até o notebook que foi chamado pela tarefa durante a execução. Este é um notebook SQL, que contém uma sequência de células SQL que são executadas em um SQL warehouse.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dependências
-- MAGIC
-- MAGIC Vamos examinar as dependências da tarefa **ingest_ProspectIncremental** visualizando o campo **Depends on**.
-- MAGIC
-- MAGIC Aqui, vemos que a tarefa depende de duas tarefas:
-- MAGIC *run_custmermgmt_YES_NO (false)* e *ingest_customermgmt*.
-- MAGIC
-- MAGIC Referindo-se ao campo **Run if dependencies**, vemos que a tarefa será executada se pelo menos uma das tarefas de dependência for bem-sucedida.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fluxo de controle de execução
-- MAGIC
-- MAGIC Os Databricks workflows fornecem construções para controlar a execução de tarefas. Uma dessas construções é a clássica *if/else*, permitindo que as tarefas sejam executadas condicionalmente.
-- MAGIC
-- MAGIC Isso é exemplificado na tarefa **run_customermgmt_YES_NO**. Condicionada à expressão apresentada no campo **Condition**, essa tarefa acionará uma condição de saída *True* ou *False*, que é declarada no campo **Depends on** das tarefas downstream.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tratamento e Relatórios de Erros
-- MAGIC
-- MAGIC À medida que os fluxos de trabalho se tornam mais complexos, o potencial de erros em tempo de execução torna-se algo que precisa ser tratado. Nesta seção, exploramos várias técnicas para manipular e/ou relatar erros.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Tratamento de erros tentando novamente
-- MAGIC
-- MAGIC Às vezes, erros transitórios podem fazer com que uma tarefa falhe, o que, por sua vez, afeta a execução do fluxo de trabalho. Para melhorar a robustez, especialmente quando as falhas podem ser devidas a problemas temporários (como disponibilidade de recursos ou interrupções de rede), as tarefas têm uma opção **retries** que fará com que a tarefa seja executada novamente automaticamente se a tarefa falhar.
-- MAGIC
-- MAGIC Identificar onde as novas tentativas devem ser aplicadas envolve determinar tarefas que tornam seu trabalho vulnerável a problemas temporários. Com as tarefas do notebook, podem ocorrer problemas como disponibilidade do cluster ou interrupções momentâneas do serviço. A configuração de novas tentativas garante que essas tarefas tentem ser executadas novamente sem intervenção manual. As tarefas SQL podem falhar ao fazendo query dos grandes datasets ou interagir com bancos de dados externos, portanto, as novas tentativas podem ajudar a resolver os tempos limite de conectividade ou execução da query.
-- MAGIC
-- MAGIC 1. Selecione uma tarefa adequada. Por exemplo, vamos escolher a tarefa **ingest_customermgmt**.
-- MAGIC 1. Localize o campo **Retries** e clique no botão **Add**.
-- MAGIC 1. Selecione um número fixo de repetições, ou *Unlimited* se não desejar impor limites ao número de repetições que podem ocorrer.
-- MAGIC 1. Especifique o tempo de espera entre as tentativas subsequentes.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Tratamento de erros através de Branching
-- MAGIC
-- MAGIC A incapacidade de executar uma tarefa com êxito pode exigir um caminho de execução alternativo. Em cenários como este, podemos aproveitar o campo **Run if dependencies** de uma tarefa downstream.
-- MAGIC
-- MAGIC A figura abaixo ilustra uma abordagem simples que nos permite lidar com um erro de inicialização executando uma tarefa específica em resposta.
-- MAGIC
-- MAGIC ![](images/exploringworkflow/error_handling_with_runif.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Relatório de Estado
-- MAGIC
-- MAGIC Você pode enviar notificações para relatar o resultado do trabalho ou da tarefa para um ou mais endereços de email ou destinos de terceiros, como Slack, Microsoft Teams, PagerDuty ou qualquer serviço baseado em webhook. 
-- MAGIC
-- MAGIC As notificações podem ser enviadas com base em qualquer um dos seguintes eventos:
-- MAGIC * Início ou execução do job / tarefa
-- MAGIC * Conclusão bem-sucedida do job / tarefa
-- MAGIC * Falha de job / tarefa
-- MAGIC * A duração do job/tarefa excedeu um limite configurado
-- MAGIC
-- MAGIC Observação: A configuração de destinos de notificação diferentes do email requer um administrador do espaço de trabalho.
-- MAGIC
-- MAGIC ![](images/exploringworkflow/notification.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Parâmetros
-- MAGIC
-- MAGIC Os parâmetros de job facilitam a reutilização e a personalização das configurações de job, permitindo que você passe pares key/valor em seus jobs para definir configurações específicas.
-- MAGIC
-- MAGIC Por exemplo, se examinarmos o código do notebook para a tarefa **ingest_customermgmt**, você notará que não há nomes codificados:
-- MAGIC    ```
-- MAGIC    CREATE TABLE IF NOT EXISTS ${catalog}.${wh_db}_${scale_factor}_stage.CustomerMgmt…
-- MAGIC    ```
-- MAGIC
-- MAGIC Instruções SQL escritas assim são muito mais portáteis, já que não há suposições codificadas sobre os nomes de catálogo ou esquema; estes podem ser passados a partir do chamador.
-- MAGIC
-- MAGIC Os Databricks Workflows facilitam a definição dos parâmetros no nível da tarefa ou do job e fazem referência a esses parâmetros no código executado usando o padrão `${parameter_name}`. Como você pode ver, os parâmetros para as tarefas atuais são derivados a partir do parâmetro do job. Durante a execução, o parâmetro referenciado será substituído pelos valores definidos na tarefa de fluxo de trabalho.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Triggering
-- MAGIC
-- MAGIC Para executar incondicionalmente o fluxo de trabalho de uma só vez, clique no botão **Run now** no canto superior direito. Isso executará todas as tarefas no fluxo de trabalho usando o SQL warehouse predeterminado. Quando todo o trabalho for concluído, você poderá ver as execuções históricas na tab **Runs**, que lista todas as execuções anteriores, bem como o estado resultante. Isso permite que você monitore e acompanhe as execuções históricas para melhorias de desempenho ou diagnósticos de erros.
-- MAGIC
-- MAGIC Vejamos algumas maneiras alternativas de trigger a execução do fluxo de trabalho.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Agendamento baseado em tempo
-- MAGIC
-- MAGIC Um caso de uso comum por trás dos fluxos de trabalho é o processamento periódico em lotes. Para atender a esse requisito, os fluxos de trabalho podem ser executados por programação.
-- MAGIC
-- MAGIC 1. Consulte o painel **Schedules & Triggers** na barra lateral direita. Clique em **Add trigger**.
-- MAGIC 1. Selecione um **Trigger Type** de *Scheduled*.
-- MAGIC 1. Dependendo dos requisitos de programação, escolha *Simple* ou *Advanced* para **Schedule type**, em seguida, especifique os detalhes da programação.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chegada do arquivo
-- MAGIC
-- MAGIC Executar um fluxo de trabalho na chegada de novos dados também é um caso de uso comum, que a Databricks acomoda.
-- MAGIC
-- MAGIC 1. Consulte o painel **Schedules & Triggers** na barra lateral direita. Clique em **Add trigger**.
-- MAGIC 1. Selecione um **Trigger Type** de *File arrival*.
-- MAGIC 1. Especifique o local a ser monitorado, em termos de volume ou local externo, ambos objetos protegíveis baseados em arquivo no Unity Catalog.
-- MAGIC 1. Se desejar, use as opções **Advanced** para implementar a limitação.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Triggering programático
-- MAGIC
-- MAGIC Há várias maneiras de acionar fluxos de trabalho programaticamente. A célula abaixo ilustra um trecho simples do Python que trigger o fluxo de trabalho usando o [Databricks SDK for Python](https://databricks-sdk-py.readthedocs.io/en/latest/index.html).
-- MAGIC
-- MAGIC Observe que a célula levará alguns instantes para ser executada, pois aguarda a conclusão da execução. Para simplesmente trigger o fluxo de trabalho de forma assíncrona, podemos substituir a invocação de API abaixo pelo seguinte:
-- MAGIC    ```
-- MAGIC    client.jobs.run_now(job_id=workflow.job_id)
-- MAGIC    ```

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from databricks.sdk import WorkspaceClient
-- MAGIC
-- MAGIC # Conectar-se ao espaço de trabalho local
-- MAGIC client = WorkspaceClient()
-- MAGIC
-- MAGIC # localize o fluxo de trabalho (nome terminado com "-TPCDI")
-- MAGIC try:
-- MAGIC     workflow = next(
-- MAGIC         filter(
-- MAGIC             lambda j: j.settings.name.endswith('-TPCDI'),
-- MAGIC             client.jobs.list()
-- MAGIC         )
-- MAGIC     )
-- MAGIC except StopIteration:
-- MAGIC     raise ValueError("Workflow not found")
-- MAGIC
-- MAGIC # trigger o fluxo de trabalho e aguardar a conclusão
-- MAGIC client.jobs.run_now_and_wait(job_id=workflow.job_id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Histórico de execução
-- MAGIC
-- MAGIC Um histórico de execuções de fluxo de trabalho é tabulado na tab **Runs**. Sinta-se à vontade para explorar esta tabela.
-- MAGIC
-- MAGIC 1. Na tab **Runs**, siga os links para uma das execuções (listadas na coluna **Start time**).
-- MAGIC 1. A partir daí, você pode explorar a execução específica selecionada. Se houve erros, isso fornece a capacidade de fazer alguma análise post-mortem para ver onde as coisas deram errado. As várias visualizações também apresentam alguma oportunidade para perfilamento e otimizar a execução.
-- MAGIC    * A view **Graph** fornece uma view DAG semelhante à vista na view **Tasks**.
-- MAGIC    * A view **Timeline** fornece um gráfico de Gantt da execução do fluxo de trabalho, fornece uma percepção visual rápida de como o tempo está sendo gasto e destaca oportunidades potenciais de paralelização.
-- MAGIC    * A view **List** fornece um resumo tabulado da execução da tarefa.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
-- MAGIC
