from worker.tasks import app
from core.models import Task 

def launch_task(project_path,*args,**kwargs):
    args = args if args else None 
    kwargs = kwargs if kwargs else None
    result = app.send_task(project_path,args=args,kwargs=kwargs)
    return result 
