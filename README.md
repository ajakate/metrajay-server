# Archived Oct 5, 2025

Archiving this, as now the main metrajay repo contains all logic for scheduling.

# MetraJay Server

backend for fetching zipped metra schedule and map info

## Development

Populate a local `.env` file with the following env variables:

```
METRA_USERNAME=
METRA_PASSWORD=
BASIC_AUTH_USERNAME=
BASIC_AUTH_PASSWORD=
```

The Metra username and password needs to be obtained from the [Metra Developer Portal](https://metra.com/developers).
The basic auth credentials can be anything.

Create a virtual env with python 3.8.2 and then run `pip install -r requirements.txt` to install deps and `python server.py` to run the app.
