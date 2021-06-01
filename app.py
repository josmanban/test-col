from logging import error
from flask import Flask, request, jsonify
from util import make_celery
from markupsafe import escape
from flask_pymongo import PyMongo
import subprocess
import pdb

app = Flask(__name__)
mongodb_client = PyMongo(app, uri="mongodb://localhost:27017/test_col")
db = mongodb_client.db
app.config.update(
    CELERY_BROKER_URL='redis://localhost:6379',
    CELERY_RESULT_BACKEND='redis://localhost:6379'
)
celery = make_celery(app)

@app.route("/new_task/", methods=['POST'])
def new_task():    
    cmd = request.form.get('cmd','')
    output = request.form.get('output','')
    status= request.form.get('status','not_started')

    new_task = db.tasks.insert_one({
        'cmd':cmd,
        'status':status,
        'output':output,
        '_id':get_next_sequence('task_id'),
    })

    run_task.delay(new_task.inserted_id)

    return {
        'id': new_task.inserted_id
    }

@app.route("/get_output/<int:id>", methods=['GET'])
def get_output(id):    
    task = db.tasks.find_one({'_id':id})
    return jsonify(task)

@celery.task
def run_task(task_id):
    task = db.tasks.find_one({'_id':task_id})
    process=subprocess.run(task.get('cmd').split(),
                       check=True,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    output = process.stdout
    err = process.stderr
    db.tasks.update_one(
        {'_id':task_id},
        {'$set':{
                'output': (output if not err else err).decode('utf-8'),
                'status':'executed'
            }
        })
    return True


def get_next_sequence(name):
    ret = db.counters.find_one_and_update(
        { '_id': name },{ '$inc': { 'seq': 1 }})
    if not ret:
        insert_result = db.counters.insert_one({
            '_id':name,
            'seq':2,
        })
        return 1
    return ret.get('seq')
