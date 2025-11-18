# .devcontainer/Dockerfile

# 使用一个标准的 Python 镜像
FROM python:3.10-slim

# 设置工作目录
WORKDIR /workspace

# 将 requirements.txt 复制到容器中
# 注意：路径是 ../ 指向根目录 (在 docker-compose 中定义)
COPY ../requirements.txt .

# Set environment variable to avoid GPL dependency
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes
# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# [可选] 安装其他你需要的 Linux 工具
# RUN apt-get update && apt-get install -y vim git

# 保持容器运行，以便 VS Code 可以附加
CMD ["sleep", "infinity"]