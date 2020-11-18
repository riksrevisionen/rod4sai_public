# Use an official Python runtime as a parent image
FROM python:3.7-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
# ADD . /app
ADD requirements.txt /app

ADD *.py /app/

RUN mkdir /app/.streamlit
ADD streamlit_config.toml /app/.streamlit/config.toml

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV NAME ROD4SAI
ENV GIT_PYTHON_REFRESH quiet

ENTRYPOINT ["streamlit", "run"]
CMD ["app.py","--server.port=80","--server.enableCORS=false"]
