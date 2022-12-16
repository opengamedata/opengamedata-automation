# opengamedata-automation
A repository for automation scripts to sync OGD logs from MySQL to BigQuery.

Setup:

* Install python3 for your environment
* Install python dependencies: "pip3 install -r requirements.txt"
* Copy `config.py.template` to `config.py` set server/authentication data and the source & destination db tables
* Download the authentication key needed for the BigQuery project. Save it as a .json file in the `config` directory and ensure the file path is defined in `config.py`

```
usage: <python> main.py

<python> is your python command.
```
