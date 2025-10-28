# Debian-based, small and stable (no Alpine)
FROM node:20-slim

# System deps for Playwright (Chromium)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates fonts-liberation libasound2 libatk1.0-0 libc6 libcairo2 libcups2 \
        libdbus-1-3 libexpat1 libfontconfig1 libgcc1 libglib2.0-0 libgdk-pixbuf2.0-0 libnspr4 libnss3 \
        libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 \
        libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 libxtst6 \
        libgbm1 xdg-utils wget gnupg && \
    rm -rf /var/lib/apt/lists/*

# Workdir
WORKDIR /usr/src/app

# Install node deps first (better layer caching)
COPY package*.json ./
RUN npm install --omit=dev

# Install Playwright Chromium (with system deps)
RUN npx playwright install --with-deps chromium

# Copy the rest of the actor
COPY . .

# Run the actor
CMD ["node", "./src/main.js"]
