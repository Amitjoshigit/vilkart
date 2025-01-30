# FROM public.ecr.aws/lambda/python:3.10
 
# # copying the actual lambda function
# # COPY connection_function.py ${LAMBDA_TASK_ROOT}/
# COPY lambda_function.py ${LAMBDA_TASK_ROOT}/
# COPY requirements.txt .
# # COPY transformation/ ${LAMBDA_TASK_ROOT}/transformation/
# RUN pip install --no-cache-dir -r requirements.txt --target ${LAMBDA_TASK_ROOT}
 
 
 
# # CMD ["connection_function.connection_handler"]    
# CMD ["lambda_function.lambda_handler"]




FROM public.ecr.aws/lambda/python:3.10

# Copy the application files
COPY . ${LAMBDA_TASK_ROOT}/

# Install dependencies
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt --target ${LAMBDA_TASK_ROOT}

# Set the CMD for the Lambda function handler
CMD ["lambda_function.lambda_handler"]
