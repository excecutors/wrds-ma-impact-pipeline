FROM python:3.11-slim

WORKDIR /workspace

COPY ../requirements.txt .

# Set environment variable to avoid GPL dependency
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes
# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

CMD ["sleep", "infinity"]