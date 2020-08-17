cp -r fp_types  ./fpcrawler/
docker-compose -f docker-compose-google.yml build fpcrawler
docker-compose -f docker-compose-google.yml push fpcrawler
rm -rf  fpcrawler/fp_types