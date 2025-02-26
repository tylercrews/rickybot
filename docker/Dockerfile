# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.12

# Set working directory
WORKDIR /var/task

# Copy dependencies and install them
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the function code
COPY rickybot_lambda_status_update.py .

# Command for AWS Lambda to run the function
CMD ["rickybot_lambda_status_update.lambda_handler"]