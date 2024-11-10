FROM frolvlad/alpine-python3:latest
WORKDIR /app
COPY requirements.txt .
COPY daily_sales_report_Netology.py .
RUN pip install -r requirements.txt
VOLUME /app/data
EXPOSE 8080
CMD ["python3", "daily_sales_report_Netology.py"]

