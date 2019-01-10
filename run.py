#!/usr/bin/python
#coding=utf-8

from app import create_app,db
from app.models import User, Role, Permission
from app.configs.config import *
app=create_app()

if __name__ == '__main__':
    app.run(port=port,threaded=True)

@app.shell_context_processor
def make_shell_context():
    return dict(db=db, User=User, Role=Role, Permission=Permission)
