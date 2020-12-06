from fastapi import APIRouter
from fastapi.responses import JSONResponse
from .forms import Item, CeleryRespone, CeleryStatusRespone
from celery_app import celery_app
from celery.result import AsyncResult


pipelineapp = APIRouter()


@pipelineapp.get("/")
def hello_world():
    return 'Hello'


@pipelineapp.post('/launch_celery_tasks', response_model=CeleryRespone)
async def tasks_post(item: Item):
    data = item.data
    try:
        async_result = celery_app.send_task(item.taskFunc, kwargs=data)
        task_id = async_result.id
        result = {'taskId': task_id}
        return JSONResponse(result)
    except Exception as e: 
        return JSONResponse(
            {'errorMessage': str(e)},
            status_code=400
        )


@pipelineapp.get('/tasks/{task_id}', response_model=CeleryStatusRespone)
async def tasks_get(task_id: str):
    try:
        res = AsyncResult(task_id)
        state = res.state
        result = res.result
        return JSONResponse({'state': state, 'data': result})
    except Exception as e: 
        return JSONResponse(
            {'errorMessage': str(e)},
            status_code=400
        )

