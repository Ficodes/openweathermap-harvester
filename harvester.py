#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2017 Future Internet Consulting and Development Solutions S.L.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import unicode_literals, with_statement, print_function

import concurrent.futures
from datetime import datetime, timedelta, timezone
import logging
import logging.handlers
import math
import os
from six.moves.urllib.parse import urljoin

import ephem
import requests
import six

BASEDIR = os.path.dirname(os.path.abspath(__file__))

CONTEXT_BROKER = 'http://localhost:1026/'
FIWARE_SERVICE = 'spain'
FIWARE_SERVICE_PATH = '/weather/forecast'

# Statistics for tracking purposes
persisted_entities = 0
in_error_entities = []

OPENWEATHERMAP_SERVICE = "http://api.openweathermap.org/data/2.5/forecast"


######

'''
GSMA enumerated values for weatherType

clearNight,
sunnyDay,
partlyCloudy,
mist,           --> neblina
fog,
cloudy,
lightRainShower,
drizzle,        --> llovizna
lightRain,
heavyRainShower,
heavyRain,
sleetShower,
sleet,         --> agua nieve
hailShower,
hail,          --> granizo
lightSnow,
shower,
lightSnow,
heavySnowShower,
heavySnow,
thunderShower,
thunder
'''

# https://openweathermap.org/weather-conditions

OWM_WEATHER_MAPPING = {
    # Thunderstorm
    200: 'lightRain, thunder',
    201: 'heavyRain, thunder',
    202: 'heavyRainShower, thunder',
    210: ', thunder',
    211: ', thunder',
    212: ', thunder',
    221: ', thunder',
    230: 'drizzle, thunder',
    231: 'drizzle, thunder',
    232: 'drizzle, thunder',

    # Drizzle
    300: 'drizzle',
    301: 'drizzle',
    302: 'drizzle',
    310: 'drizzle',
    311: 'drizzle',
    312: 'drizzle',
    313: 'drizzle',
    314: 'drizzle',
    321: 'drizzle',

    # Rain
    500: 'lightRain',
    501: 'lightRain',
    502: 'heavyRain',
    503: 'heavyRain',
    504: 'heavyRain',
    511: 'sleet',
    520: 'lightRain, shower',
    521: 'heavyRainShower',
    522: 'heavyRainShower',
    530: 'heavyRainShower',

    # Snow
    600: 'lightSnow',
    601: 'lightSnow',
    602: 'heavySnow',
    611: 'sleet',
    612: 'sleetShower',
    615: 'lightRain, lightSnow',
    616: 'lightRain, lightSnow',
    620: 'lightSnow, shower',
    621: 'lightSnow, shower',
    622: 'heavySnow, shower',

    # Atmosphere
    701: 'mist',
    711: 'smoke',
    721: 'haze',
    731: 'sand, dust whirls',
    741: 'fog',
    751: 'sand',
    761: 'dust',
    762: None,
    771: None,
    781: None,

    # Clear & clouds
    800: 'sunnyDay',
    801: 'partlyCloudy',
    802: 'partlyCloudy',
    803: 'cloudy',
    804: 'overcast',

    # Extreme
    900: None,
    901: None,
    902: None,
    903: None,
    904: None,
    905: None,
    906: None,

}

#########


def is_night(data, forecast, from_date):
    if forecast['weather'][0]['icon'].endswith('n'):
        return True
    elif forecast['weather'][0]['icon'].endswith('d'):
        return False

    sun = ephem.Sun()
    observer = ephem.Observer()
    observer.lat, observer.lon, observer.elevation = data['city']['coord']['lat'], data['city']['coord']['lon'], 667
    observer.date = from_date
    sun.compute(observer)
    return (sun.alt * 180 / math.pi) < -6


def get_weather_by_code(city_code, appid):
    out = []
    try:
        logger.info('Retrieving OpenWeatherMap data for %s' % city_code)

        response = requests.get(OPENWEATHERMAP_SERVICE, params={"id": city_code, "units": "metric", "appid": appid})
        response.raise_for_status()

        data = response.json()

        address = {
            'addressCountry': data['city']['country'],
            'addressLocality': data['city']['name']
        }

        location = {
            'type': 'Point',
            'coordinates': [data['city']['coord']['lon'], data['city']['coord']['lat']]
        }
        init_datetime = datetime.now(timezone.utc).replace(microsecond=0)
        init_date = init_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

        for forecast in data['list']:

            from_date = datetime.fromtimestamp(forecast['dt'], timezone.utc)
            to_date = from_date + timedelta(hours=3)
            index = (from_date - init_date).days

            entity = {
                'type': 'WeatherForecast',
                'id': generate_id(city_code, index, from_date, to_date),
                'address': {
                    'value': address,
                    'type': 'PostalAddress'
                },
                'location': {
                    'type': 'geo:json',
                    'value': location
                },
                'dayMaximum': {
                    'value': {
                        'relativeHumidity': forecast['main']['humidity'],
                        'temperature': forecast['main']['temp_max'],
                    }
                },
                'dayMinimum': {
                    'value': {
                        'relativeHumidity': forecast['main']['humidity'],
                        'temperature': forecast['main']['temp_min'],
                    }
                },
                #
                # Info not provided by OpenWeatherMap
                #
                # 'dateIssued': {
                #     'type': 'DateTime',
                #     'value':
                # },
                'dateRetrieved': {
                    'type': 'DateTime',
                    'value': init_datetime.isoformat()
                },
                'stationCode': {
                    'type': 'Text',
                    'value': "%s" % city_code
                },
                'temperature': {
                    'type': 'Number',
                    'value': forecast['main']['temp']
                },
                'validity': {
                    'type': 'StructuredValue',
                    'value': {
                        'from': from_date.isoformat(),
                        'to': to_date.isoformat()
                    }
                },
                # Backwards compatibility
                'validFrom': {
                    'type': 'DateTime',
                    'value': from_date.isoformat()
                },
                # Backwards compatibility
                'validTo': {
                    'type': 'DateTime',
                    'value': to_date.isoformat()
                },
                'windSpeed': {
                    'type': 'Number',
                    'value': forecast['wind']['speed']
                },
                'windDirection': {
                    'type': 'Number',
                    'value': forecast['wind']['deg']
                },
                'source': {
                    'value': 'http://openweathermap.org',
                    'type': 'URL'
                },
                'dataProvider': {
                    'value': 'Ficodes'
                }
            }

            weather_type = OWM_WEATHER_MAPPING.get(forecast['weather'][0]['id'])
            if weather_type is not None:
                if is_night(data, forecast, from_date):
                    weather_type += ', night'

                entity['weatherType'] = {
                    'type': 'Text',
                    'value': weather_type
                }

            out.append(entity)
            index += 1

    except Exception as e:
        logger.error('Error while retrieving OpenWeatherMap data for: %s. HTTP Error: %s', city_code, e)

    post_data(city_code, out)
    return len(out)


def get_weather_forecasted(codes, appid):

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

        futures = [executor.submit(get_weather_by_code, code, appid) for code in codes]
        concurrent.futures.wait(futures)


def get_parameter_data(node, periods, parameter, factor=1.0):
    param_periods = node.getElementsByTagName('dato')
    for param in param_periods:
        hour_str = param.getAttribute('hora')
        hour = int(hour_str)
        interval_start = hour - 6
        interval_start_str = str(interval_start)
        if interval_start < 10:
            interval_start_str = '0' + str(interval_start)

        period = interval_start_str + '-' + hour_str
        if param.firstChild and param.firstChild.nodeValue:
            param_val = float(param.firstChild.nodeValue)
            insert_into_period(periods, period, parameter, param_val / factor)


def insert_into_period(periods, period, attribute, value):
    periods.setdefault(period, {"period": period})
    periods[period][attribute] = {
        'value': value
    }


def generate_id(city_code, index, from_date, to_date):
    range_start = from_date.hour
    range_end = to_date.hour
    return 'OpenWeatherMap-WeatherForecast-%s-%s-%s-%s' % (city_code, index, range_start, range_end)


def post_data(city_code, data):
    if len(data) == 0:
        return

    headers = {
        'Fiware-Service': FIWARE_SERVICE,
        'Fiware-ServicePath': FIWARE_SERVICE_PATH
    }

    data_obj = {
        'actionType': 'APPEND',
        'entities': data
    }

    print("Persisting %s" % city_code)
    response = None
    url = urljoin(CONTEXT_BROKER, 'v2/op/update')
    try:
        response = requests.post(url, json=data_obj, headers=headers)
        response.raise_for_status()
        global persisted_entities
        persisted_entities = persisted_entities + len(data)
        logger.debug('Entities successfully created for location: %s', city_code)
    except Exception as e:
        if response is not None:
            logger.error('Error while POSTing data to Orion: %s' % response.content)
        else:
            logger.error('Error while POSTing data to Orion: %s' % e)
        global in_error_entities
        in_error_entities.append(city_code)


def setup_logger():
    global logger

    LOG_FILENAME = os.path.join(BASEDIR, 'harvester.log')

    # Set up a specific logger with our desired output level
    logger = logging.getLogger('WeatherForecast')
    logger.setLevel(logging.DEBUG)

    # Add a console handler
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)

    logger.addHandler(handler)

    # Add a file handler
    handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=2000000, backupCount=3)
    formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)


if __name__ == '__main__':
    import argparse

    aparser = argparse.ArgumentParser(
        description='Harvest weather forecasts from OpenWeatherMap.',
    )
    aparser.add_argument('cities', metavar='city_id', nargs='+', type=six.text_type, help='city codes to query.')
    aparser.add_argument('--cb', type=six.text_type, default=CONTEXT_BROKER, help='URL of the context broker to which data is going to be uploaded (default: %s)' % CONTEXT_BROKER)
    aparser.add_argument('--fiware-service', type=six.text_type, default="weather", help='Tenant to use for storing weather forecast data. Matchs with the FIWARE-Service header (default: weather)')
    aparser.add_argument('--fiware-service-path', type=six.text_type, default="/weather/forecast", help='Service path to store weather forecast data. Matchs with the FIWARE-ServicePath header (default: /weather/forecast)')
    aparser.add_argument('--appid', '-k', type=six.text_type, help='API key to use for connecting with OpenWeatherMap', required=True)
    args = aparser.parse_args()

    # Process command line options
    CONTEXT_BROKER = args.cb
    FIWARE_SERVICE = args.fiware_service
    FIWARE_SERVICE_PATH = args.fiware_service_path
    codes = args.cities

    # Harvest
    setup_logger()

    logger.info('#### Starting a new harvesting and harmonization cycle ... ####')
    logger.info('Number of localities to query: %d', len(codes))

    get_weather_forecasted(codes, args.appid)

    logger.info('Number of entities persisted: %d', persisted_entities)
    logger.info('Number of locations in error: %d', len(in_error_entities))
    logger.info(in_error_entities)
    logger.info('#### Harvesting cycle finished ... ####')
