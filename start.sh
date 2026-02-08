#!/bin/bash
cd "$(dirname "$0")"

echo "Starting application..."

# Build if needed
if [ ! -d "_internal/.next" ]; then
    echo "Building frontend..."
    npm --prefix _internal run build
fi

# Start both servers
cd _internal
npx concurrently "npx next start" "cd agent && uv run python main.py" \
    --names ui,agent --prefix-colors blue,green --kill-others
