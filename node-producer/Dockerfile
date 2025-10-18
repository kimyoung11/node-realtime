# Node.js 기본 이미지
FROM node:18-alpine
WORKDIR /usr/src/app
# 의존성 설치
COPY package*.json ./
RUN npm install
# 코드 복사
COPY . .
# 실행 명령어 정의
CMD ["npm", "start"]