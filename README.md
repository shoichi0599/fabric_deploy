# fabric_deploy
A python scripts using fabric for remote operations like application deployment.

## Prerequisites
- Python (2.7, 3.4+)
- Fabric 2: http://www.fabfile.org/
- Fabric 2 API Documentation: http://docs.fabfile.org/en/2.4/

## Fabric Installation
```bash
pip install fabric
fab -V
# Fabric 2.4.0
# Paramiko 2.4.2
# Invoke 1.2.0
```
For more details, see http://www.fabfile.org/installing.html

## Usage
1. Add the path of the directory to sys.path (python's module search path)
2. Import ```RemoteOperator```
```
fabric_deploy/
 │
 ├ test_app
 │  └ fabfile.py
 └ utilities/
    ├ __init__.py
    └ remote_operator.py
```
```python
# fabric_deploy/test_app/fabfile.py

# 1) To import python classes under fabric_deploy, add the path to sys.path (python's module search path
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent)) # fabric

# 2) You can import python files or directories under fabric_deploy
from lib.remote_operator import RemoteOperator
```

## Executing Fabric
Example
```python
# fabric_deploy/test_app/fabfile.py

@task
def deploy(c, env):
    # ....
    # ....
    print(env)
```
```
fab {task_name} --{argument}={value}
Example:
    fab deploy --env=pro
```

