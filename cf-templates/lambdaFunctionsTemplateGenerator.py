from cf_helpers import save_cfn_template

class LFTemplateGenerator:

    VPC_SG = 'lambdaSecurityGroup'
    SUBNET_A = 'lambdaPrivateSubnetA'
    SUBNET_B = 'lambdaPrivateSubnetB'

    LAMBDA_ROLE_NAME = 'ETLRole'
    LAMBDA_ROLE_REF = 'lambdaRole'

    CODE_S3 = 'artifactsS3'
    CODE_FOLDER = f'deployment_packages/${{deploy_time}}'

    RUNTIME = 'python3.8'

    VPC_CONFIG = []
    ENVIRONMENT_VARIABLES = []
    LAMBDA_ROLE_CONFIG = []
    APS_CONFIG = []
    LAYERS = []

    PARAMETERS = {
        CODE_S3: {
            "Type": "String",
            "Description": "S3 Bucket holding the lambda zips",
            "Include": False, ## To include in Lambda Functions Environment
        },
        CODE_FOLDER: {
            "Type": "String",
            "Description": "Setting a time so Stack gets updated",
            "Include": False,
        },
        LAMBDA_ROLE_REF: {
            "Type": "String",
            "Description": "Role to attach to the lambda functions",
            "Include": False,
        },
        VPC_SG: {
            "Type": "String",
            "Description": "Security Group to attach with Lambda",
            "Include": False,
        },
        SUBNET_A: {
            "Type": "String",
            "Description": "Private Subnet A to attach with Lambda",
            "Include": False,
        },
        SUBNET_B: {
            "Type": "String",
            "Description": "Private Subnet B to attach with Lambda",
            "Include": False,
        },
        "accessKey": {
            "Name": "ACCESS_KEY",
            "Type": "String",
            "Description": "AWS IAM Access Key for accessing other AWS resources",
            "Include": True,
        },
        "appEnv": {
            "Name": "APP_ENV",
            "Type": "String",
            "Description": "Denote the environment",
            "Include": True,
        },
        "appUrl": {
            "Name": "APP_URL",
            "Type": "String",
            "Description": "URL to Access the FLOW API",
            "Include": True,
        },
        "awsAccountId": {
            "Name": "AWS_ACCOUNT_ID",
            "Type": "String",
            "Description": "Account ID where the lambda functions are located",
            "Include": True,
        },
        "bucketName": {
            "Name": "BUCKET_NAME",
            "Type": "String",
            "Description": "S3 Bucket holding the statements",
            "Include": True,
        },
        "internalApiToken": {
            "Name": "INTERNAL_API_TOKEN",
            "Type": "String",
            "Description": "Token to access FLOW API",
            "Include": True,
        },
        "internalApiUsername": {
            "Name": "INTERNAL_API_USERNAME",
            "Type": "String",
            "Description": "Username to access FLOW API",
            "Include": True,
        },
        "regionName": {
            "Name": "REGION_NAME",
            "Type": "String",
            "Description": "AWS Region name",
            "Include": True,
        },
        "secretKey": {
            "Name": "SECRET_KEY",
            "Type": "String",
            "Description": "AWS IAM Secret Key for accessing other AWS resources",
            "Include": True,
        },
        "stmtDbDatabase": {
            "Name": "STMT_DB_DATABASE",
            "Type": "String",
            "Description": "Statement Database Name",
            "Include": True,
        },
        "stmtDbHost": {
            "Name": "STMT_DB_HOST",
            "Type": "String",
            "Description": "Statement Database Host",
            "Include": True,
        },
        "stmtDbPassword": {
            "Name": "STMT_DB_PASSWORD",
            "Type": "String",
            "Description": "Statement Database Password",
            "Include": True,
        },
        "stmtDbUsername": {
            "Name": "STMT_DB_USERNAME",
            "Type": "String",
            "Description": "Statement Database Username",
            "Include": True,
        },
    }

    def set_lambda_role(self):
        lambda_role = {
            "Fn::Sub": [
                "arn:aws:iam::${AWS::AccountId}:role/${" + self.LAMBDA_ROLE_NAME +"}",
                {
                    self.LAMBDA_ROLE_NAME: {
                        "Ref": self.LAMBDA_ROLE_REF
                    }
                }
            ]
        }
        self.LAMBDA_ROLE_CONFIG = lambda_role

    def set_vpc_config(self):
        vpc_config = {
            "SecurityGroupIds": [
                {
                    "Ref": self.VPC_SG
                }
            ],
            "SubnetIds": [
                {
                    "Ref": self.SUBNET_A
                },
                {
                    "Ref": self.SUBNET_B
                }
            ]
        }
        self.VPC_CONFIG = vpc_config

    def set_environment_variables(self):

        environment_config = dict()
        env_variables = dict()
        parameters = self.PARAMETERS

        for ref in parameters.keys():
            if parameters[ref]['Include']:
                env_name = parameters[ref]['Name']
                env_variables[env_name] = {
                    "Ref": ref
                }

        environment_config['Variables'] = env_variables
        self.ENVIRONMENT_VARIABLES = environment_config
    
    def get_s3_zip(self, zip_name):
    
        s3_zip = {
            "S3Bucket": {
                "Ref": self.CODE_S3
            },
            "S3Key": {
                "Fn::Sub": f"{self.CODE_FOLDER}/{zip_name}.zip"
            }
        }

        return s3_zip

    def generate_lambda_properties(self, function_name, handler, time, memory):
        
        lamda_function_properties = dict()
        lamda_function_properties['FunctionName'] = function_name
        lamda_function_properties['Runtime'] = self.RUNTIME
        lamda_function_properties['Role'] = self.LAMBDA_ROLE_CONFIG
        lamda_function_properties['Handler'] = handler
        lamda_function_properties['Code'] = self.get_s3_zip(function_name)
        lamda_function_properties['Timeout'] = time
        lamda_function_properties['MemorySize'] = memory
        lamda_function_properties['Environment'] = self.ENVIRONMENT_VARIABLES
        lamda_function_properties['Layers'] = self.LAMBDA_LAYERS_CONFIG
        lamda_function_properties['VpcConfig'] = self.VPC_CONFIG
        return lamda_function_properties

    def generate_lambda_function(self, function_name, handler, time, memory):

        lamda_function_template = dict() 

        lamda_function_template['Type'] = 'AWS::Lambda::Function'
        lamda_function_template['Properties'] = self.generate_lambda_properties(function_name, handler, time, memory)
        return lamda_function_template
    
    def get_lambda_layer(self, layer_name):
        return {
            "Type": "AWS::Lambda::LayerVersion",
            "Properties": {
                "CompatibleRuntimes": [
                    self.RUNTIME
                ],
                "Content": self.get_s3_zip(layer_name),
                "LayerName": layer_name
            }
        }

    def set_layers_template(self, resources):
        layers = self.LAYERS
        layers_template = dict()
        lambda_layers_config = list()

        for layer_name in layers:
            layers_template[layer_name] = self.get_lambda_layer(layer_name)
            lambda_layers_config.append({
                'Ref': layer_name
            })

        self.LAMBDA_LAYERS_CONFIG = lambda_layers_config
        resources.update(layers_template)
        return resources

    def set_configs(self):
        self.set_environment_variables()
        self.set_lambda_role()
        self.set_vpc_config()

    def set_lambda_template(self, resources):
        aps_config = self.APS_CONFIG
        for ap_config in aps_config:
            function = ap_config['function']
            handler = ap_config['handler']
            time = ap_config['time']
            memory = ap_config['memory']
            function_id = function.title().replace("_", "")

            resources[function_id] = self.generate_lambda_function(function, handler, time, memory)
        return resources

    def get_parameters(self):
        parameters = self.PARAMETERS
        entries_to_remove = ['Name', 'Include']
        parameters_to_set = dict()
        for parameter_name in parameters:
            parameter = parameters[parameter_name]
            for entry_to_remove in entries_to_remove:
                parameter.pop(entry_to_remove, None)
            parameters_to_set[parameter_name] = parameter
        return parameters_to_set

    def generate_resources(self):
        resources = dict()
        resources = self.set_layers_template(resources)
        resources = self.set_lambda_template(resources)
        return resources

    def __call__(self):
        self.set_configs()
        cfn_template = dict()
        cfn_template['AWSTemplateFormatVersion'] = "2010-09-09"
        cfn_template['Description'] = 'ETL Lambda Functions CloudFormation Template'
        cfn_template['Parameters'] = self.get_parameters()
        cfn_template['Resources'] = self.generate_resources()
        save_cfn_template(cfn_template, 'cf-templates/templates/ETLLambdaFunctions.json')

    def __init__(self, aps_config, layers):
        self.APS_CONFIG = aps_config
        self.LAYERS = layers

  
def main():
    aps_config = [
        {
            'time': 300,
            'memory': 800,
            'function': 'rmtn_export',
            'handler': 'rmtn_export.lambda_handler',
        },
        {
            'time': 300,
            'memory': 800,
            'function': 'rmtn_transform',
            'handler': 'rmtn_transform.lambda_handler2',
        },
        {
            'time': 300,
            'memory': 2000,
            'function': 'rbok_export',
            'handler': 'rbok_export.lambda_handler',
        },
        {
            'time': 300,
            'memory': 800,
            'function': 'rbok_transform',
            'handler': 'rbok_transform.lambda_handler2',
        },
        {
            'time': 300,
            'memory': 800,
            'function': 'score_calc',
            'handler': 'score.score_calc',
        },
        {
            'time': 300,
            'memory': 800,
            'function': 'ratl_export',
            'handler': 'ratl_export.lambda_handler',
        },
        {
            'time': 300,
            'memory': 800,
            'function': 'ratl_transform',
            'handler': 'ratl_transform.lambda_handler2',
        },
        {
            'time': 300,
            'memory': 800,
            'function': 'rrtn_export',
            'handler': 'rrtn_export.lambda_handler',
        },
        {
            'time': 300,
            'memory': 800,
            'function': 'rrtn_transform',
            'handler': 'rrtn_transform.lambda_handler2',
        },
    ]
    layers = ['PythonPackages']

    template = LFTemplateGenerator(aps_config, layers)
    template()

if __name__ == '__main__':
    main()