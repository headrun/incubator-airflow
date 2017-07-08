# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import traceback

import airflow.api

from airflow.api.common.experimental import trigger_dag as trigger
from airflow.api.common.experimental.get_task import get_task
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.exceptions import AirflowException
from airflow.www.app import csrf

from flask import (
    g, Markup, Blueprint, redirect, jsonify, abort,
    request, current_app, send_file, url_for
)
from datetime import datetime

_log = logging.getLogger(__name__)

requires_authentication = airflow.api.api_auth.requires_authentication

api_experimental = Blueprint('api_experimental', __name__)

import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

import json

@csrf.exempt
@api_experimental.route('/createuser/', methods=['POST'])
def createuser():
    userdata = json.loads(request.data)
    username = userdata.get('user') or ''
    email = userdata.get('email') or ''
    password = userdata.get('password') or username + '123'
    superuser = userdata.get('superuser') or False
    if not superuser:
        superuser = 0
    else:
        superuser = 1

    data = {}
    try:
        if not username:
            data['status'] = '200'
            data['message'] = "Username has missed, unable to create user in Airflow server"
            return jsonify(data)

        session = settings.Session()
        user = session.query(models.User).filter(models.User.username == username).first()
        if user:
            user.superuser = superuser
            session.commit()

            data['status'] = '200'
            data['message'] = 'SuperUser status is updated successfully!'
        else:
            user = PasswordUser(models.User())
            user.username = username
            user.email = email
            user.password = password
            user.superuser = superuser
            session.add(user)
            session.commit()

            data['status'] = '200'
            data['message'] = "User Created successfully!"

    except Exception as e:
        data['status'] = '400'
        data['message'] = e.message

    return jsonify(data)

@csrf.exempt
@api_experimental.route('/deleteuser/', methods = ['POST'])
def deleteuser():
    data = {}
    try:
        session = settings.Session()
        data = json.loads(request.data)
        username = data['username']
        user = session.query(models.User).filter(models.User.username == username)
        user.delete()
        session.commit()

        data['message'] = 'User has deleted successfully!'
        data['status'] = '200'
    except Exception as e:
        data['status'] = '400'
        data['message'] = e.message

    return jsonify(data)

@csrf.exempt
@api_experimental.route('/updatetasks/', methods=['POST'])
def updatetasks():
    data = {}
    try:
        tasks_data = json.loads(request.data)
        with open('/root/airflow/dags/clients.json', 'w') as outfile:
            json.dump(tasks_data, outfile)

        data['status'] = '200'
        data['message'] = "Updated task successfully!"
    except Exception as e:
        data['status'] = '400'
        data['message'] = e.message

    return jsonify(data)

@csrf.exempt
@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['POST'])
@requires_authentication
def trigger_dag(dag_id):
    """
    Trigger a new dag run for a Dag with an execution date of now unless
    specified in the data.
    """
    data = request.get_json(force=True)

    run_id = None
    if 'run_id' in data:
        run_id = data['run_id']

    conf = None
    if 'conf' in data:
        conf = data['conf']

    execution_date = None
    if 'execution_date' in data and data['execution_date'] is not None:
        execution_date = data['execution_date']

        # Convert string datetime into actual datetime
        try:
            execution_date = datetime.strptime(execution_date,
                                               '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2015-11-16T14:34:15'
                .format(execution_date))
            _log.info(error_message)
            response = jsonify({'error': error_message})
            response.status_code = 400

            return response

    try:
        dr = trigger.trigger_dag(dag_id, run_id, conf, execution_date)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 404
        return response

    if getattr(g, 'user', None):
        _log.info("User {} created {}".format(g.user, dr))

    response = jsonify(message="Created {}".format(dr))
    return response


@api_experimental.route('/test', methods=['GET'])
@requires_authentication
def test():
    return jsonify(status='OK')


@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>', methods=['GET'])
@requires_authentication
def task_info(dag_id, task_id):
    """Returns a JSON with a task's public instance variables. """

    try:
        info = get_task(dag_id, task_id)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 404
        return response

    # JSONify and return.
    fields = {k: str(v)
              for k, v in vars(info).items()
              if not k.startswith('_')}
    return jsonify(fields)


@api_experimental.route('/dags/<string:dag_id>/dag_runs/<string:execution_date>/tasks/<string:task_id>', methods=['GET'])
@requires_authentication
def task_instance_info(dag_id, execution_date, task_id):
    """
    Returns a JSON with a task instance's public instance variables.
    The format for the exec_date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
    of course need to have been encoded for URL in the request.
    """

    # Convert string datetime into actual datetime
    try:
        execution_date = datetime.strptime(execution_date,
                                           '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15'
            .format(execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response

    try:
        info = get_task_instance(dag_id, task_id, execution_date)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 404
        return response

    # JSONify and return.
    fields = {k: str(v)
              for k, v in vars(info).items()
              if not k.startswith('_')}
    return jsonify(fields)


@api_experimental.route('/latest_runs', methods=['GET'])
@requires_authentication
def latest_dag_runs():
    """Returns the latest running DagRun for each DAG formatted for the UI. """
    from airflow.models import DagRun
    dagruns = DagRun.get_latest_runs()
    payload = []
    for dagrun in dagruns:
        if dagrun.execution_date:
            payload.append({
                'dag_id': dagrun.dag_id,
                'execution_date': dagrun.execution_date.strftime("%Y-%m-%d %H:%M"),
                'start_date': ((dagrun.start_date or '') and
                               dagrun.start_date.strftime("%Y-%m-%d %H:%M")),
                'dag_run_url': url_for('airflow.graph', dag_id=dagrun.dag_id,
                                       execution_date=dagrun.execution_date)
            })
    return jsonify(items=payload) # old flask versions dont support jsonifying arrays
