#! /bin/bash

(echo '*/5 * * * * curl $1 -u "$2"') | crontab -
service cron start
