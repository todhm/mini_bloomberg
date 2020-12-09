from flask.cli import FlaskGroup
from application import create_app
from mongoengine import connect


app = create_app()
connect(     
    host=app.config.get('MONGO_URI'),
    db=app.config.get('MONGODB_NAME')
)
cli = FlaskGroup(create_app=create_app)


if __name__ == '__main__':
    cli()