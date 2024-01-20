# Paradise SS13 API Scraper
This repository is a small Python tool for quickly and efficiently extracting data from the Paradise SS13 public stats API to be mirrored in a local PostgreSQL database. (https://api.paradisestation.org/stats)

Responses from the playercount and blackbox endpoints are stored as JSONb objects to enable use of Postgre's efficient JSON querying tools.