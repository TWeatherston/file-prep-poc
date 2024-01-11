from fastapi import FastAPI
import uvicorn

from .subscriptions import router as subscriptions_router
from . import tasks, schema

app = FastAPI(
    title="File Preparer",
    description="Service for preparing files, ready for transformer processing",
    lifespan=subscriptions_router.lifespan_context,
)
app.include_router(subscriptions_router)


@app.post("/prepare", response_model=schema.TaskResult)
def prepare(prepare_file: schema.PrepareFile):
    task = tasks.prepare_file.delay(**prepare_file.model_dump())
    return {"task_id": task.id}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
