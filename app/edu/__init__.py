from flask import Blueprint

edu = Blueprint('edu', __name__)

from . import views
