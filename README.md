# opengamedata-automation

A repository for automation scripts to sync OGD logs from MySQL to BigQuery.

Setup:

* Install python3 for your environment
* Install python dependencies: "pip3 install -r requirements.txt"
* Copy `config.py.template` to `config.py` set server/authentication data and the source & destination db tables
* Download the authentication key needed for the BigQuery project. Save it as a .json file in the `config` directory and ensure the file path is defined in `config.py`

```bash
usage: <python> main.py <game> --max-days <count>

<python> is your python command.
<game> is the game whose data you wish to move to BigQuery
<count> is the max number of days-worth of data you wish to move
```

These processes are also set up to run automatically in GitHub actions.
Current workflows are configured to run at the following times:

| Time (UTC) | Time (Central) | Time (Central, Daylight) | Game                 |
| ---        | ---            | ---                      | ---                  |
|  9:00      | 3:00 (AM)      | 4:00 (AM)                | AQUALAB              |
|  9:05      | 3:05 (AM)      | 4:05 (AM)                | BACTERIA             |
| 10:45      | 4:45 (AM)      | 5:45 (AM)                | BALLOON              |
|  9:10      | 3:10 (AM)      | 4:10 (AM)                | BLOOM                |
|  9:15      | 3:15 (AM)      | 4:15 (AM)                | CRYSTAL              |
|  9:20      | 3:20 (AM)      | 4:20 (AM)                | CYCLE_CARBON         |
|  9:25      | 3:25 (AM)      | 4:25 (AM)                | CYCLE_NITROGEN       |
|  9:30      | 3:30 (AM)      | 4:30 (AM)                | CYCLE_WATER          |
|  9:35      | 3:35 (AM)      | 4:35 (AM)                | EARTHQUAKE           |
|  9:40      | 3:40 (AM)      | 4:40 (AM)                | ICECUBE              |
|  9:45      | 3:45 (AM)      | 4:45 (AM)                | JOURNALISM           |
|  9:50      | 3:50 (AM)      | 4:50 (AM)                | JOWILDER             |
|  9:55      | 3:55 (AM)      | 4:55 (AM)                | LAKELAND             |
| 10:00      | 4:00 (AM)      | 5:00 (AM)                | MAGNET               |
| 10:05      | 4:05 (AM)      | 5:05 (AM)                | MASHOPOLIS           |
| 11:05      | 5:05 (AM)      | 6:05 (AM)                | MATCH                |
| 10:10      | 4:10 (AM)      | 5:10 (AM)                | PENGUINS             |
| 10:15      | 4:15 (AM)      | 5:15 (AM)                | SHADOWSPECT          |
| 10:20      | 4:20 (AM)      | 5:20 (AM)                | SHIPWRECKS           |
| 10:55      | 4:55 (AM)      | 5:55 (AM)                | SLIDE                |
| 11:00      | 5:00 (AM)      | 6:00 (AM)                | STACK                |
| 10:25      | 4:25 (AM)      | 5:25 (AM)                | THERMOVR             |
| 10:30      | 4:30 (AM)      | 5:30 (AM)                | TRANSFORMATION_QUEST |
| 10:35      | 4:35 (AM)      | 5:35 (AM)                | WAVES                |
| 10:50      | 4:50 (AM)      | 5:50 (AM)                | WEATHER_STATION      |
| 10:40      | 4:40 (AM)      | 5:40 (AM)                | WIND                 |

Last keep-alive on 08/01/24
