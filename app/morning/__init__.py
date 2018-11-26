from flask import Blueprint

morning = Blueprint('morning', __name__)

from . import views
