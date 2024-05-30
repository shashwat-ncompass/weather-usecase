from airflow.models.taskinstance import TaskInstance as BaseTaskInstance

class TaskInstance(BaseTaskInstance):
    __allow_unmapped__ = True