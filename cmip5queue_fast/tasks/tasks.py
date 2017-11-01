from celery.task import task
from dockertask import docker_task
import subprocess
import requests
import os
import shutil
import json as jsonx

#Default base directory 
basedir="/data/static_web"

@task()
def check_task_storage(args):
    """
        Checks task storage space from a specific task
        args: user_id, task_id
        return size in bytes
    """
    task_id = args['task_id']
    user_id = args['user_id']
    resultDir = os.path.join(basedir, 'cmip5_tasks/', user_id, task_id, 'output/')
    task_space = get_size(start_path = resultDir)
    return task_space

@task()
def delete_task_data(args):    
    """
        Deletes user storage space from a specific task
        args: {user_id, history_task_id}
        note: history_task_id is from a previously completed task. This is the celery id.
        returns space deleted in bytes
        example: {"user_id":"duncan", "task_id":"c57b560e-e46c-4eca-85e7-e856e871a2e8"}
        function deletes entire subfolder "/output" from the specified task folder
    """
    task_id = args['history_task_id']
    user_id = args['user_id']
    taskDir = os.path.join(basedir, 'cmip5_tasks/', user_id, task_id, 'output/')
    deleted_size = get_size(start_path = taskDir)
    shutil.rmtree(taskDir)
    return deleted_size

@task()
def create_tables(args):
    """
        Runs R script to create html tables of the CMIP5 data selected
        args are:
              {user_id,
              final_query}
    """
    user_id = args['user_id']
    resultDir = setup_user_directory(user_id)
    # write the args to disk. These contain shell characters '$' so passing as arguments doesn't work
    with open(resultDir + '/tables.json', "wt") as f:
        jsonx.dump(args,f)
    command = 'Rscript'
    path2script = '/data/cmip5_functions/cmip5_tables.R'
    pass_args = [user_id]
    cmd = [command, '--vanilla', path2script] + pass_args
    subprocess.call(cmd)    
    return
    
def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size
            	
def setup_user_directory(user_id):
    resultDir = os.path.join(basedir, 'cmip5_tasks/', user_id)
    try:
        os.makedirs(resultDir)
        os.makedirs("{0}/tables".format(resultDir))
    except OSError as err:
        if err.errno!=17:
            raise
    os.chmod(resultDir,0777)
    os.chmod("{0}/tables".format(resultDir),0777)
    return resultDir

