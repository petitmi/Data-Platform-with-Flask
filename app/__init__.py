from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_bootstrap import Bootstrap
from app.configs.config import *
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
bootstrap = Bootstrap()
#用户认证
login_manager=LoginManager()
login_manager.session_protection = 'strong'  # 认证加密程度
login_manager.login_view = 'auth.login'  # 登陆认证的处理视图
login_manager.login_message = u'对不起，本页只能登录者访问'  # 登陆提示信息
REMOTE_HOST = "https://pyecharts.github.io/assets/js"

db = SQLAlchemy()

from app import models
from app.main import views

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    Config.init_app(app)
    bootstrap.init_app(app)
    login_manager.init_app(app)  # 配置用户认证信息
    login_manager.login_message_category = 'info'
    db.init_app(app)

    app.jinja_env.auto_reload = True
    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)
    from .edu import edu as edu_blueprint
    app.register_blueprint(edu_blueprint)
    from .ec import ec as ec_blueprint
    app.register_blueprint(ec_blueprint)
    from .circle import circle as circle_blueprint
    app.register_blueprint(circle_blueprint)
    from .morning import morning as morning_blueprint
    app.register_blueprint(morning_blueprint)
    from .api import api as api_blueprint
    app.register_blueprint(api_blueprint)
    from .auth import auth as auth_blueprint
    app.register_blueprint(auth_blueprint, url_prefix='/auth')

    return app