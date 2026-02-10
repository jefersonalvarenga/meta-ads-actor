# Uses the official Apify Playwright/Firefox image
# See https://hub.docker.com/r/apify/actor-node-playwright-chrome for more info
FROM apify/actor-node-playwright-chrome:20

# Copy package files first for Docker layer caching
COPY package*.json ./

# Install npm dependencies, skip optional and avoid running Playwright install
# since it's already bundled in the image
RUN npm --quiet set progress=false \
    && npm install --only=prod --no-optional \
    && echo "All npm dependencies installed"

# Copy the rest of the source code
COPY . ./

# Build TypeScript
RUN npm run build

# Specify default command
CMD npm run start
