To generate new server controller/model code from the spec:

1. Run
`openapi-generator generate -i ./spec/barman_api.yaml -o ./generated -g python-flask`

(For more information on openapi-generator, look at docs here: https://github.com/OpenAPITools/openapi-generator)


2. Copy the relevant files into the appropriate subdirectories of `/server`. These will be any new or updated files in `generated/openapi-server/controllers` and `generated/openapi-server/models`. 

3. Update the imports in these files accordingly

4. Fill in the controller methods with actual logic (they are generated with a stub "do some magic" return response)
