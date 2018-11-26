from flask_login import UserMixin, AnonymousUserMixin,current_user
from . import db, login_manager


class Permission:
    ADMIN = 7
    EDU=2
    EC=1
    CIRCLE=4
    INDEX=8
    SUPER=15



class Role(db.Model):
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), unique=True)
    permissions = db.Column(db.Integer)
    users = db.relationship('User', backref='role', lazy='dynamic')

    def __init__(self, **kwargs):
        super(Role, self).__init__(**kwargs)
        if self.permissions is None:
            self.permissions = 0

    @staticmethod
    def insert_roles():
        roles = {
            'EDU_er': Permission.EDU,
            'EC_er':Permission.EC,
            'Circle_er': Permission.CIRCLE,
            'Administrator': Permission.EC| Permission.EDU|Permission.CIRCLE|Permission.ADMIN,
            'Admin_super':Permission.EC| Permission.EDU|Permission.CIRCLE|Permission.ADMIN|Permission.INDEX|Permission.SUPER
        }
        for r in roles:
            role = Role.query.filter_by(name=r).first()
            if role is None:
                role = Role(name=r)
            role.permissions = roles[r]
            db.session.add(role)
        db.session.commit()


    def __repr__(self):
        return '<Role %r>' % self.name



class User(UserMixin, db.Model):
    __tablename__ = 'users'#对应mysql数据库表
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64))
    password = db.Column(db.String(64))
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))

    def __init__(self,**kwargs):
        super(User, self).__init__(**kwargs)
        if self.role is None:
            if self.name == 'xmmz':
                self.role = Role.query.filter_by(permissions=15).first()
            if self.role is None:
                self.role = Role.query.filter_by(permissions=1).first()

    def can(self, permissions):
        return self.role is not None and (self.role.permissions & permissions) == permissions

    def is_admin(self):
        return self.can(Permission.ADMIN)

    def __repr__(self):
        return '<User %r>' % self.username


class AnonymousUser(AnonymousUserMixin):
    def can(self, permissions):
        return False

    def is_administrator(self):
        return False

login_manager.anonymous_user = AnonymousUser


@login_manager.user_loader
def load_user(username):
    return User.query.get(username)

class edu(db.Model):
    __tablename__ = 'edu_orders'#对应mysql数据库表
    id = db.Column('id',db.Integer, primary_key=True)
    order_id = db.Column('order_id',db.Integer)
    classroom_id = db.Column('classroom_id',db.Integer)
    class_title = db.Column('class_title',db.Text)
    teacher_name=db.Column('teacher_name',db.Text)
    pay_time=db.Column('pay_time',db.DateTime)
    orderamount=db.Column('orderamount',db.Integer)
    public_price=db.Column('public_price',db.Integer)
    wallet_pay=db.Column('wallet_pay',db.Integer)



