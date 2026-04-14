# import os
# from flask import Flask
# from .db import init_app


# def create_app():
#     app = Flask(__name__, instance_relative_config=True)
#     app.config.from_object('config.Config')

#     os.makedirs(app.instance_path, exist_ok=True)

#     init_app(app)

#     from .routes.auth import auth_bp
#     from .routes.main import main_bp

#     app.register_blueprint(auth_bp)
#     app.register_blueprint(main_bp)

#     return app
import os
from flask import Flask
from .db import init_app

# ✅ FIXED IMPORT (from routes.main)
from .routes.main import init_scheduler


def create_app():
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object('config.Config')

    os.makedirs(app.instance_path, exist_ok=True)

    init_app(app)

    from .routes.auth import auth_bp
    from .routes.main import main_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)

    # ✅ START SCHEDULER
    init_scheduler(app)

    return app