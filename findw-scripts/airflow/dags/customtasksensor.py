from airflow.sensors.base import BaseSensorOperator
from airflow.models import DagRun, TaskInstance
from airflow.utils.db import provide_session
from airflow.exceptions import AirflowSkipException

class TimeRangeExternalTaskSensor(BaseSensorOperator):

    template_fields = ['external_dag_id', 'external_task_id', 'allowed_states']

    @provide_session
    def poke(self, session=None, **kwargs):
        external_dag_id = kwargs['external_dag_id']
        external_task_id = kwargs['external_task_id']
        allowed_states = kwargs['allowed_states']
        execution_date = kwargs['logical_date']
        print("Current DAG execution_date:", execution_date)
        start_execution_date = execution_date.subtract(hours=12)
        print("12 Hours Before Current DAG's execution_date:", start_execution_date)
        #check current state of dag run based on their recent execution.
        query_all = session.query(DagRun).filter(DagRun.dag_id == external_dag_id, DagRun.execution_date > start_execution_date
        ).order_by(DagRun.execution_date.desc()).all()

        session.commit()
        i = 0
        if query_all:
            while i < len(query_all):
                query = query_all[i]
                if query.state == 'running' or query.state == 'failed':
                    print("External Dag:", external_dag_id, "Details:", query)
                    print("External Dag:", external_dag_id, "State:", query.state)
                    print("External Dag:", external_dag_id, "execution_date:", query.execution_date)
                    TI = TaskInstance
                    success_task_count = session.query(TI).filter(
                        TI.dag_id == external_dag_id,
                        TI.task_id == external_task_id,
                        TI.state.in_(allowed_states),
                        TI.execution_date == query.execution_date
                    ).count()
                    other_task_count = session.query(TI).filter(
                        TI.dag_id == external_dag_id,
                        TI.task_id == external_task_id,
                        TI.state.notin_(allowed_states),
                        TI.execution_date == query.execution_date
                    ).count()
                    none_task_count = session.query(TI).filter(
                        TI.dag_id == external_dag_id,
                        TI.task_id == external_task_id,
                        TI.state == None,
                        TI.execution_date == query.execution_date
                    ).count()
                    print("Success Count for External Dag", external_dag_id, "and Task", external_task_id, ":", success_task_count)
                    print("Other Not in allowed_states Count for External Dag (external_dag_id) and Task (external_task_id):", other_task_count)
                    print("Other (state is None) Count for External Dag", external_dag_id, "and Task", external_task_id, ":", none_task_count)
                    if other_task_count > 0 or none_task_count > 0:
                        raise AirflowSkipException
                else:
                    print(f"External Dag (external_dag_id)'s execution state is {query.state} and execution date is {query.execution_date}")
                    i += 1
        else:
            print(f"External Dag (external_dag_id), has not been triggered/does not exist from Execution Start time ({start_execution_date})")
        return True