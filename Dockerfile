FROM apache/airflow:2.5.1

# 명령어들을 root 사용자로 실행
USER root

# OS 업데이트 및 필요한 패키지 설치
RUN apt-get update
RUN apt-get install -y default-libmysqlclient-dev gcc

# 명령어들을 airflow 사용자로 실행
USER airflow
RUN pip install --ignore-installed "apache-airflow-providers-mysql"
RUN pip install urllib3==1.26.5
RUN pip install requests==2.26.0


# 필요한 Python 패키지 설치
RUN pip install yfinance oauth2client gspread


# 로컬 파일시스템 파일 복사
COPY . /app
