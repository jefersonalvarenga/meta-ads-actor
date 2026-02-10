# Uses the official Apify Playwright/Firefox image
# See https://hub.docker.com/r/apify/actor-node-playwright-chrome for more info
FROM apify/actor-node-playwright-chrome:20

# Copy package files first for Docker layer caching
COPY package*.json ./

# Install all dependencies (including devDependencies) to have tsc available
RUN npm --quiet set progress=false \
    && npm install --no-optional \
    && echo "All npm dependencies installed"

# Copy the rest of the source code
COPY . ./

# Build TypeScript, then remove devDependencies to keep image lean
RUN npm run build \
    && npm prune --production

# Specify default command
CMD npm run start
