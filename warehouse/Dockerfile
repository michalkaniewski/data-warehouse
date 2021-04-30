FROM postgres:latest
ENV POSTGRES_PASSWORD=1q2w3e4r
ENV POSTGRES_USER=postgres
ENV POSTGRES_DB=warehouse
COPY init.sql /docker-entrypoint-initdb.d/init.sql