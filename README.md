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

| Time (UTC) | Time (Central) | Time (Central, Daylight) | Game                 | Status  |
| ---        | ---            | ---                      | ---                  | ---     |
|  9:00      | 3:00 (AM)      | 4:00 (AM)                | AQUALAB              | ![Aqualab](https://github.com/opengamedata/opengamedata-automation/actions/workflows/aqualab.yml/badge.svg) |
|  9:05      | 3:05 (AM)      | 4:05 (AM)                | BACTERIA             | ![Bacteria](https://github.com/opengamedata/opengamedata-automation/actions/workflows/bacteria.yml/badge.svg) |
| 10:45      | 4:45 (AM)      | 5:45 (AM)                | BALLOON              | ![Balloon](https://github.com/opengamedata/opengamedata-automation/actions/workflows/balloon.yml/badge.svg) |
|  9:10      | 3:10 (AM)      | 4:10 (AM)                | BLOOM                | ![Bloom](https://github.com/opengamedata/opengamedata-automation/actions/workflows/bloom.yml/badge.svg) |
|  9:15      | 3:15 (AM)      | 4:15 (AM)                | CRYSTAL              | ![Crystal](https://github.com/opengamedata/opengamedata-automation/actions/workflows/crystal.yml/badge.svg) |
|  9:20      | 3:20 (AM)      | 4:20 (AM)                | CYCLE_CARBON         | ![Carbon Cycle](https://github.com/opengamedata/opengamedata-automation/actions/workflows/cycle_carbon.yml/badge.svg) |
|  9:25      | 3:25 (AM)      | 4:25 (AM)                | CYCLE_NITROGEN       | ![Nitrogen Cycle](https://github.com/opengamedata/opengamedata-automation/actions/workflows/cycle_nitrogen.yml/badge.svg) |
|  9:30      | 3:30 (AM)      | 4:30 (AM)                | CYCLE_WATER          | ![Water Cycle](https://github.com/opengamedata/opengamedata-automation/actions/workflows/cycle_water.yml/badge.svg) |
|  9:35      | 3:35 (AM)      | 4:35 (AM)                | EARTHQUAKE           | ![Earthquake](https://github.com/opengamedata/opengamedata-automation/actions/workflows/earthquake.yml/badge.svg) |
|  9:40      | 3:40 (AM)      | 4:40 (AM)                | ICECUBE              | ![Icecube](https://github.com/opengamedata/opengamedata-automation/actions/workflows/icecube.yml/badge.svg) |
|  9:45      | 3:45 (AM)      | 4:45 (AM)                | JOURNALISM           | ![Journalism](https://github.com/opengamedata/opengamedata-automation/actions/workflows/journalism.yml/badge.svg) |
|  9:50      | 3:50 (AM)      | 4:50 (AM)                | JOWILDER             | ![Jo Wilder](https://github.com/opengamedata/opengamedata-automation/actions/workflows/jowilder.yml/badge.svg) |
|  9:55      | 3:55 (AM)      | 4:55 (AM)                | LAKELAND             | ![Lakeland](https://github.com/opengamedata/opengamedata-automation/actions/workflows/lakeland.yml/badge.svg) |
| 10:00      | 4:00 (AM)      | 5:00 (AM)                | MAGNET               | ![Magnet](https://github.com/opengamedata/opengamedata-automation/actions/workflows/magnet.yml/badge.svg) |
| 10:05      | 4:05 (AM)      | 5:05 (AM)                | MASHOPOLIS           | ![Mashopolis](https://github.com/opengamedata/opengamedata-automation/actions/workflows/mashopolis.yml/badge.svg) |
| 11:05      | 5:05 (AM)      | 6:05 (AM)                | MATCH                | ![Match](https://github.com/opengamedata/opengamedata-automation/actions/workflows/match.yml/badge.svg) |
| 10:10      | 4:10 (AM)      | 5:10 (AM)                | PENGUINS             | ![Penguins](https://github.com/opengamedata/opengamedata-automation/actions/workflows/penguins.yml/badge.svg) |
| 10:15      | 4:15 (AM)      | 5:15 (AM)                | SHADOWSPECT          | ![Shadowspect](https://github.com/opengamedata/opengamedata-automation/actions/workflows/shadowspect.yml/badge.svg) |
| 10:20      | 4:20 (AM)      | 5:20 (AM)                | SHIPWRECKS           | ![Shipwrecks](https://github.com/opengamedata/opengamedata-automation/actions/workflows/shipwrecks.yml/badge.svg) |
| 10:55      | 4:55 (AM)      | 5:55 (AM)                | SLIDE                | ![Slide](https://github.com/opengamedata/opengamedata-automation/actions/workflows/slide.yml/badge.svg) |
| 11:00      | 5:00 (AM)      | 6:00 (AM)                | STACK                | ![Stack](https://github.com/opengamedata/opengamedata-automation/actions/workflows/stack.yml/badge.svg) |
| 10:25      | 4:25 (AM)      | 5:25 (AM)                | THERMOLAB            | ![Thermo Lab](https://github.com/opengamedata/opengamedata-automation/actions/workflows/thermolab.yml/badge.svg) |
| 10:30      | 4:30 (AM)      | 5:30 (AM)                | TRANSFORMATION_QUEST | ![Transformations Quest](https://github.com/opengamedata/opengamedata-automation/actions/workflows/transformation_quest.yml/badge.svg) |
| 10:35      | 4:35 (AM)      | 5:35 (AM)                | WAVES                | ![Waves](https://github.com/opengamedata/opengamedata-automation/actions/workflows/waves.yml/badge.svg) |
| 10:50      | 4:50 (AM)      | 5:50 (AM)                | WEATHER_STATION      | ![Weather Station](https://github.com/opengamedata/opengamedata-automation/actions/workflows/weather_station.yml/badge.svg) |
| 10:40      | 4:40 (AM)      | 5:40 (AM)                | WIND                 | ![Wind](https://github.com/opengamedata/opengamedata-automation/actions/workflows/wind.yml/badge.svg) |

Last keep-alive on 08/01/24
