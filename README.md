# Pipeline Lakehouse com Apache Airflow e PySpark

## Descrição do Projeto

Essa atividade implementa um pipeline de dados baseado na arquitetura Lakehouse, utilizando PySpark para processamento distribuído e Apache Airflow.

O fluxo realiza:
- Ingestão de dados em formato JSON
- Transformação em múltiplas camadas (Landing → Bronze → Silver → Gold)
- Consolidação de dados analíticos
- Orquestração automatizada com DAG no Airflow

---

## Arquitetura Lakehouse

A estrutura segue o padrão de camadas:


Landing → Bronze → Silver → Gold


### Estrutura de diretórios


BULK/
 └── lakehouse/
 ├── landing/ # Dados brutos (JSON)
 ├── bronze/ # Conversão JSON → Parquet
 ├── silver/ # Limpeza e padronização
 └── gold/ # Dados consolidados (analítico)


---

## Tecnologias utilizadas

- Python 3.12
- Apache Spark (PySpark)
- Apache Airflow
- Linux (Ubuntu)
- Parquet (formato otimizado)

---

## Etapas do Pipeline

### 1. Landing
Armazenamento dos arquivos JSON:
- customers
- orders
- order_items

---

### 2. Bronze
Conversão dos JSON para Parquet.

Scripts:
- bronze_customers.py
- bronze_orders.py
- bronze_order_items.py

Objetivo:
- Padronizar formato
- Melhorar performance de leitura

---

### 3. Silver
Limpeza e tratamento dos dados:
- Remoção de prefixos
- Padronização de colunas

Scripts:
- silver_customers.py
- silver_orders.py
- silver_order_items.py

Resultado:
- Dados estruturados e consistentes

---

### 4. Gold
Consolidação final dos dados.

Script:
- gold.py

Geração de dataset analítico contendo:
- city
- state
- quantidade_pedidos
- valor_total_pedido

---

## Resultado final

Local:

lakehouse/gold/final/


Arquivos gerados:
- _SUCCESS
- part-xxxxx.parquet

Indica execução concluída com sucesso.

---

## Orquestração com Airflow

Foi criada uma DAG chamada:


lakehouse_pipeline


Funcionalidades:
- Executa os 7 scripts (3 Bronze, 3 Silver, 1 Gold)
- Paraleliza a execução das camadas Bronze e Silver
- Garante dependência para execução do Gold após o Silver

---

## Execução

### Execução manual

```bash
spark-submit bronze/bronze_customers.py
spark-submit bronze/bronze_orders.py
spark-submit bronze/bronze_order_items.py

spark-submit silver/silver_customers.py
spark-submit silver/silver_orders.py
spark-submit silver/silver_order_items.py

spark-submit gold/gold.py
Execução com Airflow
airflow standalone

Acessar:

http://localhost:8080

Passos:

Ativar a DAG lakehouse_pipeline
Executar (Trigger DAG)
Tempo de execução
Execução manual: aproximadamente 2 a 3 minutos
Execução via Airflow: aproximadamente 2 a 4 minutos
Validação

O pipeline é considerado correto quando:

Todas as camadas são geradas

Existe a pasta:

lakehouse/gold/final/
Contém:
_SUCCESS
arquivos .parquet
Conclusão

O projeto implementa:

Arquitetura Lakehouse
Processamento distribuído com PySpark
Orquestração com Airflow
Pipeline completo e automatizado
