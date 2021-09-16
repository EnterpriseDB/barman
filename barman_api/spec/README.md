To generate new server controller/model code from the spec:

1. Run
`openapi-generator generate -i ./spec/barman_api.yaml -o ./generated -g python-flask`

(For more information on openapi-generator, look at docs here: https://github.com/OpenAPITools/openapi-generator)


2. Copy the relevant files (or parts of files, if you're extending an existing controller) into the appropriate subdirectories of `/server`. These will be any new or updated files in `generated/openapi-server/controllers` and `generated/openapi-server/models`. 

3. Update the names and imports in these files accordingly.

For example, connexion needs the operationId of a path to be fully qualified, ie "server.controllers.utility_controller.diagnose", which will cause openapi-generator to produce a method called "server_controllers_utility_controller_diagnose". This method name can be safely shortened to simply "diagnose". 

4. Fill in the controller methods with actual logic (they are generated with a stub "do some magic" return response)
