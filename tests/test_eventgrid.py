import pytest
import subprocess
import os
import signal
import requests
import json
import time
import collections 
### Travis: http://luisquintanilla.me/2018/02/18/testing-deploying-python-projects-travisci/
#func host start and then open a new terminal and cd into the test folder
# pytest -v test_eventgrid.py
## https://learning.oreilly.com/library/view/python-testing-with/9781680502848/f_0011.xhtml#ch.pytest
# The Task structure is used as a data structure to pass information between the UI and the API
#Task = collections.namedtuple(​'Task'​, [​'summary'​, ​'owner'​, ​'done'​, ​'id'​])
pro = None

#Task = collections.namedtuple(​'Task'​, [​'summary'​, ​'owner'​, ​'done'​, ​'id'​])
# You can use __new__.__defaults__ to create Task objects without having to specify all the fields.
#Task.__new__.__defaults__ = (None, None, False, None)


@pytest.fixture
def init_func():
    pass
    #subprocess.Popen("func host start",shell=True)
    # The os.setsid() is passed in the argument preexec_fn so
    # it's run after the fork() and before  exec() to run the shell.
    #pro = subprocess.Popen(['func','host','start'],stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid) 
    #yield
    #print("tearing down functions host...")
    #os.killpg(os.getpgid(pro.pid), signal.SIGTERM)


# https://docs.pytest.org/en/latest/fixture.html
# https://docs.pytest.org/en/latest/parametrize.html
# https://learning.oreilly.com/library/view/Python+Testing+with+pytest/9781680502848/f_0026.xhtml#parametrized_testing
# Use @pytest.mark.parametrize(argnames, argvalues) to pass lots of data through the same test, like this:
@pytest.mark.parametrize('web', ['http://localhost:7071/api/GE_Clean_Trigger',
                                'http://localhost:7071/api/MTU_Clean_Trigger',
                                'http://localhost:7071/api/PO_Match'])
#@pytest.fixture
#def web():
#        links = 'http://localhost:7071/api/GE_Clean_Trigger'
#        return links

def test_eg_validation(init_func, web):
        with open('subvalidation.json') as f:
                payload = json.load(f)
                r = requests.post(web, json = payload)
                print(r.status_code,r.json())
                assert 'validationResponse' in str(r.json())
