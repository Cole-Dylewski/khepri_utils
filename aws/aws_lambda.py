


def context_to_json(context):
    return {
        "function_name": context.function_name,
        "function_version": context.function_version,
        "invoked_function_arn": context.invoked_function_arn,
        "memory_limit_in_mb": context.memory_limit_in_mb,
        "aws_request_id": context.aws_request_id,
        "log_stream_name": context.log_stream_name,
        "log_group_name": context.log_group_name
    }
    
def get_log_link(context):
    if isinstance(context, dict):
        region = context['invoked_function_arn'].split(':')[3]
        log_stream_name = context['log_stream_name'].replace('$','$2524').replace('/','$252F').replace('[','$255B').replace(']','$255D')
        log_group_name =f"$252Faws$252Flambda$252F{context['function_name']}" 
        # context['log_group_name'].replace('/', '$252F')
        log_url = 'https://' + region + '.console.aws.amazon.com/cloudwatch/home?region=' + region + '#logsV2:log-groups/log-group/'+ log_group_name  + '/log-events/' + log_stream_name
        
    else:
        region = context.invoked_function_arn.split(':')[3]
        log_stream_name = context.log_stream_name.replace('$','$2524').replace('/','$252F').replace('[','$255B').replace(']','$255D')
        log_group_name = context.log_group_name.replace('/', '$252F')
        log_url = 'https://' + region + '.console.aws.amazon.com/cloudwatch/home?region=' + region + '#logsV2:log-groups/log-group/'+ log_group_name  + '/log-events/' + log_stream_name
    
    return log_url

def get_lambda_environment_variables(lambda_function_name):
    import boto3
    """
    Retrieve environment variables of an AWS Lambda function.

    Args:
        lambda_function_name (str): Name of the Lambda function.

    Returns:
        dict: A dictionary containing the environment variables of the Lambda function.
              Returns None if the function does not exist or if an error occurs.
    """
    try:
        # Create a Lambda client
        lambda_client = boto3.client('lambda')

        # Retrieve the function configuration, including environment variables
        response = lambda_client.get_function_configuration(FunctionName=lambda_function_name)

        # Extract and return the environment variables
        return response.get('Environment', {}).get('Variables')
    except Exception as e:
        print("An error occurred:", e)
        return None
    
def update_lambda_environment_variables(lambda_function_name, environment_variables):
    import boto3
    """
    Update environment variables of an AWS Lambda function.

    Args:
        lambda_function_name (str): Name of the Lambda function.
        environment_variables (dict): A dictionary containing the new environment variables.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        # Create a Lambda client
        lambda_client = boto3.client('lambda')

        # Update the function's environment variables
        response = lambda_client.update_function_configuration(
            FunctionName=lambda_function_name,
            Environment={'Variables': environment_variables}
        )

        # Check if the update was successful
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            print("Failed to update environment variables.")
            return False
    except Exception as e:
        print("An error occurred:", e)
        return False
