from functools import wraps
from flask import abort
from flask_login import current_user
from .models import Permission
from flask import render_template

def permission_required(permission):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.can(permission):
                return render_template('contact_me.html')
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def admin_required(f):
    return permission_required(Permission.ADMIN)(f)
