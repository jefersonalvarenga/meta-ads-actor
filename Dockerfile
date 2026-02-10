# Uses the official Apify Playwright/Firefox image
# See https://hub.docker.com/r/apify/actor-node-playwright-chrome for more info
FROM apify/actor-node-playwright-chrome:20

# Copy package files first for Docker layer caching
COPY package*.json ./

# Install production dependencies (typescript is a dependency, not devDependency)
RUN npm --quiet set progress=false \
    && npm install --omit=dev \
    && echo "All npm dependencies installed"

# Copy the rest of the source code
COPY . ./

# Build TypeScript
RUN node_modules/.bin/tsc -p tsconfig.json

# Specify default command
CMD npm run start
