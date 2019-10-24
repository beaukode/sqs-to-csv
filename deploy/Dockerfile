FROM node:10-alpine

COPY dist/*.js /app/
COPY dist/*.json /app/
COPY dist/LICENSE /app/

WORKDIR /app

RUN npm install --production

CMD ["/usr/local/bin/node", "index.js"]