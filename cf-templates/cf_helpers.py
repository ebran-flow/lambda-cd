import json

def save_cfn_template(cfn_template, save_path):
        with open(save_path, 'w') as outfile:
            outfile.write(json.dumps(cfn_template))