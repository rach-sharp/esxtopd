FROM python:3
ADD esxtopd.py Pipfile Pipfile.lock /code/
WORKDIR /code
RUN pip3 install pipenv && pipenv install --system
CMD ["python3", "-u", "esxtopd.py"]
