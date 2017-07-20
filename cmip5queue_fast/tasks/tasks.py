from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
from shutil import copyfile, move
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
def check_user_storage(args):
    """
        Checks user storage space from previous tasks
        args: user_id
        example {"user_id":"duncan"}
        return size in bytes
    """
    user_id = args['user_id']
    resultDir = os.path.join(basedir, 'cmip5_tasks/', user_id)
    user_space = get_size(start_path = resultDir)
    return user_space    

@task()
def delete_task_data(args):    
    """
        Deletes user storage space from a specific task
        args: {user_id, History_task_id}
        note: history_task_id is from a previously completed task
        returns space deleted in bytes
        example: {"user_id":"duncan", "task_id":"trialrun"}
        function deletes entire subfolder "/output" from a task folder
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
    task_id = str(create_tables.request.id)
    user_id = args['user_id']
    resultDir = setup_user_directory(user_id)
    with open(resultDir + '/tables.json', "wt") as f:
        jsonx.dump(args,f)
    docker_opts = "-v /data/cmip5_functions:/sccsc:ro -v /data/static_web/cmip5_tasks:/sccsc_out -w /sccsc"
    docker_cmd ="Rscript /sccsc/cmip5_tables.R {0}".format(user_id)
    result = docker_task(docker_name="sccsc/r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    return result
    
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
    except OSError as err:
        if err.errno!=17:
            raise
    os.chmod(resultDir,0777)
    return resultDir

