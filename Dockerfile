FROM python:3.12-slim-bookworm

WORKDIR /app

# Install dependencies for the installer
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# Download the latest installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer, then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

# Copy necessary files
COPY pyproject.toml .
COPY src/ src/

# Install dependencies defined in `pyproject.toml` globally (not in a virtual environment)
RUN uv pip install -r pyproject.toml --system

# Expose the port for FastAPI
EXPOSE 8000

# Command to run the FastAPI application with a specified host for external access
CMD ["fastapi", "dev", "--host", "0.0.0.0", "src/app/main.py"]

