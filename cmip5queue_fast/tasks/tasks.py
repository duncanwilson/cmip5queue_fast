from celery.task import task
from subprocess import call,STDOUT
from shutil import copyfile, move
import requests
import os
import shutil

#Default base directory 
basedir="/data/static_web"

@task()
def check_user_storage(args):
    """
        Checks user storage space from previous tasks
        args: user id
        return in bytes
    """
    user_id = args['user_id']
    resultDir = os.path.join(basedir, 'cmip5_tasks/', user_id)
    user_space = get_size(start_path = resultDir)
    return user_space

@task()
def delete_task_data(args):    
    """
        Checks user storage space from previous tasks
        args: user id, task_id
        return in bytes
    """
    task_id = args['task_id']
    user_id = args['user_id']
    taskDir = os.path.join(basedir, 'cmip5_tasks/', user_id, task_id, 'output/')
    deleted_size = get_size(start_path = taskDir)
    shutil.rmtree(taskDir)
    return deleted_size

def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

