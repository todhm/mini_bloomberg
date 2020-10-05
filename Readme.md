docker-compose file
docker-compose exec connector flask index create-index
docker-compose exec connector flask index create-report-data

http://localhost:9500/haproxy_stats 에서 torproxy admin가능
username:haproxy
password: password