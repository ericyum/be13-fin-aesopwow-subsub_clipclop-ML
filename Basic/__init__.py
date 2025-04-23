from flask import Flask
from Basic.controller.Basic_controller import Basic_bp  # basic controller blueprint

app = Flask(__name__)

# Blueprint 등록
app.register_blueprint(Basic_bp, url_prefix='/basic')

if __name__ == '__main__':
    app.run(debug=True)
