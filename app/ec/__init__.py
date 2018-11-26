from flask import Blueprint

ec = Blueprint('ec', __name__)

from . import views
