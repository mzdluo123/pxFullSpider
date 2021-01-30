FROM python:3.7.7
COPY requirements.txt /tmp/requirements.txt
VOLUME ["/data"]
WORKDIR /data
RUN cd /tmp &&\
 pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt
CMD cd /data && python new.py