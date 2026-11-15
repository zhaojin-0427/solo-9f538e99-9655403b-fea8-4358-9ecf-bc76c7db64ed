FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
# 示例停机备份（含待同步更正）随镜像提供，打开 /restore 即可试用
COPY samples ./samples

ENV DATA_DIR=/data \
    PORT=5000 \
    SYNC_POLL_INTERVAL=0.5

# 持久化由 compose 的两个独立卷负责（source-data / target-data）；
# 恢复用的上传暂存与切换状态分别写入两库各自目录。
EXPOSE 5000

CMD ["python", "-m", "app"]
