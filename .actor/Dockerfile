# ✅ Use Debian-based Node image (NOT Alpine)
FROM node:20-slim

# Install system dependencies required by Playwright
RUN apt-get update && \
    apt-get install -y wget gnupg ca-certificates fonts-liberation libasound2 libatk1.0-0 libc6 libcairo2 libcups2 \
    libdbus-1-3 libexpat1 libfontconfig1 libgcc1 libglib2.0-0 libgdk-pixbuf2.0-0 libnspr4 libnss3 \
    libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 \
    libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 libxtst6 \
    libgbm1 xdg-utils && \
    rm -rf /var/lib/apt/lists/*

# Install Playwright browsers
RUN npm install -g playwright && \
    npx playwright install --with-deps chromium

# Copy actor files
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install --omit=dev
COPY . .

# Default run command
CMD ["node", "./src/main.js"]
