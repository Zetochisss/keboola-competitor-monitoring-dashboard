#!/bin/bash
set -Eeuo pipefail
cd /app && npm ci --omit=dev
