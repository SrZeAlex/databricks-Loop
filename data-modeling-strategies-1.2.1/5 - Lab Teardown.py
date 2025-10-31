# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Desmontagem de Laboratório

# COMMAND ----------

# MAGIC %md
# MAGIC Parabéns! Esperamos que você tenha gostado desse conjunto de atividades de laboratório, projetado para lhe dar uma noção de quão facilmente você pode oferecer suporte a uma variedade de metodologias de modelagem de dados entrelaçadas para data warehouse e casos de uso modernos na Databricks.
# MAGIC
# MAGIC Encorajamos você a continuar sua jornada de aprendizado com cursos adicionais no caminho do Arquiteto de Dados Associado. Como um lembrete adicional, você pode estar interessado em explorar outros caminhos de aprendizagem, como Engenheiro de Dados e Engenheiro de ML, para um treinamento técnico mais profundo nas disciplinas de maior interesse para você.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨OBRIGATÓRIO - SELECT CLASSIC COMPUTE
# MAGIC
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
# MAGIC #### Antes de ir nós ficaríamos gratos se você usasse o script de desmontagem de laboratório para arrumar os artefatos de suas atividades de laboratório.

# COMMAND ----------

# DBTITLE 1,%run lab_teardown
# MAGIC %run ./Includes/teardown/lab_teardown

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obrigado por participar deste laboratório!

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
