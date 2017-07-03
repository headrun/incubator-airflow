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

import airflow.api

from airflow.api.common.experimental import trigger_dag as trigger
from airflow.api.common.experimental.get_task import get_task
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.exceptions import AirflowException
from airflow.www.app import csrf
from flask import render_template, request, flash, session
from flask.sessions import *

import flask_login
from flask_login import login_required, current_user, logout_user


from flask import (
    g, Markup, Blueprint, redirect, jsonify, abort,
    request, current_app, send_file, url_for
)

from datetime import datetime

_log = logging.getLogger(__name__)

requires_authentication = airflow.api.api_auth.requires_authentication

api_custom_pw_auth = Blueprint('api_custom_pw_auth', __name__)

import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

@api_custom_pw_auth.route('/createuser/', methods=['GET'])
def createuser():
    username = request.args.get('user')
    email = request.args.get('email', '')
    superuser = request.args.get('superuser', False)

    if superuser == 'False':
        superuser = 0
    else:
        superuser = 1

    try:
        user = PasswordUser(models.User())
        user.username = username
        user.email = email
        user.password = username + '123'
        user.superuser = superuser
        session = settings.Session()
        session.add(user)
        session.commit() 

        return jsonify("User Created")
    except:
        return jsonify("Raised Exception")

@api_custom_pw_auth.route('/test', methods=['GET'])
@requires_authentication
def test():
    return jsonify(status='OK')

@api_custom_pw_auth.route('/login/', methods=['POST', 'GET'])
@csrf.exempt
def login():

    username = None
    password = None


    if request.method == 'GET':
        username = request.args.get("username")
        password = request.args.get("password") or username + '123'
        get_response = request.args.get("get_response") or None

    if request.method == 'POST':
        username = request.values.get("username")
        password = request.values.get("password")
        get_response = request.values.get("get_response") or None        

    if not username or not password:
        return jsonify(status={
            'type': 'fail',
            'msg': 'Insufficient arguments',
            'code': 422})

    try:
        db_session = settings.Session()
        user = db_session.query(PasswordUser).filter(
            PasswordUser.username == username).first()

        if not user:
            db_session.close()
            # raise AuthenticationError()

        if not user.authenticate(password):
            db_session.close()
            # raise AuthenticationError()

        # LOG.info("User %s successfully authenticated", username)

        status = flask_login.login_user(user)
        if get_response == '_true_':
            session_id, session_token = get_current_cookie(current_app, session)
            resp = jsonify(status={
                'type': 'success',
                'msg': 'Login Successfull',
                'session': session_token,
                'session_id': session_id,
                'code': 200})
        else:
            resp = jsonify(status={
                'type': 'success',
                'msg': 'Login Successfull',
                'code': 200})            

        db_session.commit()
        db_session.close()

        return resp
    except Exception as e:
        # flash("Incorrect login details")
        return jsonify(status='Incorrect login details')


@api_custom_pw_auth.route('/logout/', methods=['POST', 'GET'])
@csrf.exempt
def logout():
    try:
        get_response = request.values.get("get_response") or None
        logout_user()
        session_id, session_token = get_current_cookie(current_app, session)

        if get_response == '_true_':
            session_id, session_token = get_current_cookie(current_app, session)
            resp = jsonify(status={
                'type': 'success',
                'msg': 'Logout Successfull',
                'session': session_token,
                'session_id': session_id,
                'code': 200})
        else:
            resp = jsonify(status={
                'type': 'success',
                'msg': 'Logout Successfull',
                'code': 200})           

        return resp
    except Exception as e:
        return jsonify(status={
            'type': 'fail',
            'msg': e.message,
            'code': 422})

def get_current_cookie(current_app, session):
    cookieinterface_obj = SecureCookieSessionInterface()
    session_token = cookieinterface_obj.get_signing_serializer(current_app).dumps(dict(session))
    session_id = session['_id']
    return [session_id, session_token]
    
