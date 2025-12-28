"""
DAG de exemplo: compara dois valores e ramifica para tarefas diferentes.

Como usar:
- Valores padrão: a=5, b=3
- Para executar com valores customizados, dispare a DAG com conf JSON, por exemplo:
  airflow dags trigger compare_values --conf '{"a":10, "b":4}'
"""


from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator


def _get_values(**context):
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", {}) or {}
    a = conf.get("a", 5)
    b = conf.get("b", 3)
    logging.info(f"Comparando a={a} e b={b}")

    if a == b:
        return "equal_task"
    elif a > b:
        return "greater_task"
    else:
        return "less_task"

def _log_equal(**context):
    logging.info("Os valores são iguais")


def _log_greater(**context):
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", {}) or {}
    a = conf.get("a", 5)
    b = conf.get("b", 3)
    logging.info(f"{a} é maior que {b}")


def _log_less(**context):
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", {}) or {}
    a = conf.get("a", 5)
    b = conf.get("b", 3)
    logging.info(f"{a} é menor que {b}")


with DAG(
    dag_id="compare_values",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    branch_compare = BranchPythonOperator(
        task_id="branch_compare",
        python_callable=_get_values,
    )

    equal_task = PythonOperator(
        task_id="equal_task",
        python_callable=_log_equal,
    )

    greater_task = PythonOperator(
        task_id="greater_task",
        python_callable=_log_greater,
    )

    less_task = PythonOperator(
        task_id="less_task",
        python_callable=_log_less,
    )

    branch_compare >> [equal_task, greater_task, less_task]
