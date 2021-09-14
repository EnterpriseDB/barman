import os
import connexion
from server import encoder

def main():
    app = connexion.App(__name__, specification_dir='./spec/')
    app.app.json_encoder = encoder.JSONEncoder
    app.add_api('barman_rest_api.yaml',
                arguments={'title': 'Barman REST API'},
                pythonic_params=True)

    app.run(port=8080)

if __name__ == '__main__':
    main()