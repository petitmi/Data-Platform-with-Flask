from flask import Blueprint

circle = Blueprint('circle', __name__)

from . import views
