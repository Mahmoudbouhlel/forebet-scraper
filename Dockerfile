FROM python:3.12-slim

# Install required system packages
RUN apt-get update && apt-get install -y \
    wget unzip gnupg curl ca-certificates \
    fonts-liberation libappindicator3-1 libasound2 libatk-bridge2.0-0 \
    libatk1.0-0 libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 libnspr4 libnss3 \
    libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libgbm1 \
    libgtk-3-0 libxshmfence1 lsb-release xvfb --no-install-recommends && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Google Chrome
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    dpkg -i google-chrome*.deb || apt-get -fy install && \
    rm google-chrome*.deb

# Install specific version of Chromedriver (v114 matches well with Chrome stable)
RUN wget -q https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip && \
    unzip chromedriver_linux64.zip && \
    mv chromedriver /usr/local/bin/ && chmod +x /usr/local/bin/chromedriver && \
    rm chromedriver_linux64.zip

# Set display environment for headless Chrome
ENV DISPLAY=:99

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your app code into the container
COPY . /app
WORKDIR /app

# Run the script (adjust if your entrypoint is different)
CMD ["python", "flash.py", "--days", "2", "--excel"]
