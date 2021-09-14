To run the Flask app:

Manually (either on local dev machine or from a barman server)
if on an actual server, must do this as the `barman` user so barman can actually access the other servers
`python run.py` or `python3 run.py`

For a real deployment
tba


Technology notes and other reference material

For the process of generating app stub code from an openapi spec, and how Flask and Connexion and everything goes together in the process (Note that it uses an older version of OAS and associated tools; here we're using OAS 3.0.0 and correspondingly openapi-generator, not swagger-codegen.)
https://haseebmajid.dev/blog/rest-api-openapi-flask-connexion

