FROM --platform=linux/amd64 python:3.10 
ADD main.py .
RUN pip install "snowflake-snowpark-python[pandas]"
CMD ["python", "-u","./main.py"] 