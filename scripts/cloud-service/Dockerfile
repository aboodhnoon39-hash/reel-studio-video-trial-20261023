FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/* && mkdir -p /data /app/scripts/cloud-service /app/scripts/dev-app && chown -R node:node /data /app
WORKDIR /app
COPY --chown=node:node scripts/cloud-service/server.mjs scripts/cloud-service/server.mjs
COPY --chown=node:node scripts/dev-app/cloud-media-pipeline.mjs scripts/dev-app/cloud-media-pipeline.mjs
ENV HOST=0.0.0.0 PORT=7860 CLOUD_STORAGE_ROOT=/data
USER node
EXPOSE 7860
CMD ["node","scripts/cloud-service/server.mjs"]
