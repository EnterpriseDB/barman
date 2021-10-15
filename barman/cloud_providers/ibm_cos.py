from barman.cloud_providers.aws_s3 import S3CloudInterface

try:
    import ibm_boto3 as boto3
    from ibm_botocore.exceptions import ClientError, EndpointConnectionError
    from ibm_botocore.client import Config, ClientError
except ImportError:
    raise SystemExit("Missing required python module: boto3")


class COSCloudInterface(S3CloudInterface):
    def __init___(self, url, **kwargs):
        super(COSCloudInterface, self).__init__(
            url=url,
            **kwargs,
        )

    def _reinit_session(self):
        """
        Create a new session
        """
        ibm_api_key_id = "value of apikey in IBM COS JSON credential"
        ibm_service_instance_id = (
            "value of resource_instance_id in IBM COS JSON credential"
        )
        session = boto3.Session(
            ibm_api_key_id=ibm_api_key_id,
            ibm_service_instance_id=ibm_service_instance_id,
        )
        self.s3 = session.resource(
            "s3",
            endpoint_url=self.endpoint_url,
            config=Config(signature_version="oauth"),
        )
