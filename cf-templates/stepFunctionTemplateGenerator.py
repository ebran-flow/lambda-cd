from cf_helpers import save_cfn_template

class SFTemplateGenerator:

    ALL_CHOICE_STATES_CONFIG = []
    LAMBDA_STATES = []
    TIMEOUT = 1200
    SCORE_STATE = 'score_calc'
    START_AT = 'initial_choice'
    RETRY_BLOCK = [
        {
            "ErrorEquals": [
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
                "Lambda.TooManyRequestsException",
            ],
            "IntervalSeconds": 2,
            "MaxAttempts": 6,
            "BackoffRate": 2,
        }
    ]
    
    def generate_pass_state(self, next_state):
        state = {
            "Type": "Pass",
        }
        state = self.set_next_state_r_end(state, next_state)
        return state

    def set_next_state_r_end(self, state, next_state):
        if next_state:
            state['Next'] = next_state
        else:
            state['End'] = True
        return state

    def generate_lambda_state(self, current_state, next_state):
        
        state = {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "OutputPath": "$.Payload",
            "Parameters": {
                "Payload.$": "$",
                "FunctionName": {
                    "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:" + f"{current_state}:$LATEST"
                }
            },
            "Retry": self.RETRY_BLOCK,
        }
        state = self.set_next_state_r_end(state, next_state)

        return state

    def generate_choice_state(self, variable_to_check, condition, next_state):
        state_config = {
            "Variable": f"$.{variable_to_check}",
            "StringEquals": condition,
            "Next": next_state,
        }
        return state_config

    def generate_choice_block(self, choice_states_config):
        choice_config = dict()
        choices = list()
        
        for choice_state_config in choice_states_config:
            choice_state = self.generate_choice_state(choice_state_config['variable_to_check'], choice_state_config['condition'], choice_state_config['next_state'])
            choices.append(choice_state)

        choice_config['Type'] = 'Choice'
        choice_config['Choices'] = choices
        choice_config['Default'] = 'Fail'
        return choice_config

    def generate_all_states(self):
        states = dict()
        for choice_name, choice_state_config in self.ALL_CHOICE_STATES_CONFIG.items():
            states[choice_name] = self.generate_choice_block(choice_state_config)

        states['Fail'] = {"Type": "Fail"}
        states['Pass'] = self.generate_pass_state(None)

        for LAMBDA_STATE in self.LAMBDA_STATES:
            current_state = LAMBDA_STATE['current_state']
            next_state = LAMBDA_STATE['next_state']
            states[current_state] = self.generate_lambda_state(current_state, next_state)
        return states

    def generate_sfn_template(self):
        template = dict()
        template['Comment'] = 'Step Function to handle ETL workflow'
        template['StartAt'] = self.START_AT
        template['States'] = self.generate_all_states()
        template['TimeoutSeconds'] = self.TIMEOUT

        return template

    def generate_choice_config(self, variable_to_check, condition, next_state):
        return {
            'variable_to_check': variable_to_check,
            'condition': condition,
            'next_state': next_state
        }

    def generate_lambda_state_config(self, current_state, next_state):
        return {
            'current_state': current_state,
            'next_state': next_state
        }
        
    def set_acc_prvdr_choices(self, all_choice_states, choice_1, choice_2, next_state_1, next_state_2):
        all_choice_states[choice_1] = [self.generate_choice_config('status', 'success', next_state_1)]
        all_choice_states[choice_2] = [self.generate_choice_config('status', 'success', next_state_2)]
        return all_choice_states

    def set_acc_prvdr_lambda_states(self, export_state, transform_state, choice_1, choice_2):
        return [ 
            self.generate_lambda_state_config(export_state, choice_1),
            self.generate_lambda_state_config(transform_state, choice_2),
        ]
        
    def set_lambda_n_choice_state_configs(self, ap_codes):
        initial_choices = list()
        all_choice_states = dict()
 
        lambda_states = [ 
            self.generate_lambda_state_config(self.SCORE_STATE, 'score_choice')
        ]
        all_choice_states['score_choice'] = [self.generate_choice_config('status', 'success', 'Pass')]

        for ap_code in ap_codes:
            ap_code_l = ap_code.lower()
            export_state = f'{ap_code_l}_export'
            transform_state = f'{ap_code_l}_transform'
            choice_1 = f'{ap_code}_choice_1'
            choice_2 = f'{ap_code}_choice_2'

            state = self.set_acc_prvdr_lambda_states(export_state, transform_state, choice_1, choice_2)
            all_choice_states = self.set_acc_prvdr_choices(all_choice_states ,choice_1, choice_2, transform_state, self.SCORE_STATE)
            
            choice = self.generate_choice_config('acc_prvdr_code', ap_code, f"{ap_code_l}_export")
            initial_choices.append(choice)
            lambda_states.extend(state)

        all_choice_states[self.START_AT] = initial_choices
        self.LAMBDA_STATES = lambda_states
        self.ALL_CHOICE_STATES_CONFIG = all_choice_states

    def generate_cfn_template(self, sfn_template):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Description": "Stack which generates the StepMachine for ETL Lambda Functions",
            "Resources": {
                "ETLStateMachine": {
                    "Type": "AWS::StepFunctions::StateMachine",
                    "Properties" : {
                        "StateMachineName": "ETLStateMachine",
                        "Definition" : sfn_template,
                        "RoleArn": {
                            "Fn::Sub": "arn:aws:iam::${AWS::AccountId}:role/ETLSfnRole"
                        }         
                    }
                }
            }
        }
    
    def __call__(self):
        sfn_template = self.generate_sfn_template()
        cfn_template = self.generate_cfn_template(sfn_template)
        save_cfn_template(cfn_template, 'cf-templates/templates/ETLStepFunctions.json')

    def __init__(self, ap_codes):
        self.AP_CODES = ap_codes
        self.set_lambda_n_choice_state_configs(ap_codes)
    
def main():
    template = SFTemplateGenerator(['RMTN','RBOK','RATL', 'RRTN'])
    template()

if __name__ == '__main__':
    main()