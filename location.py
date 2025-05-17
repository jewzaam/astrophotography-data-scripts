"""
This module provides a class to represent and manage location data for astrophotography.
It includes attributes for latitude, longitude, and other location-specific details.
"""

import re
import time
import traceback
import requests 

import common
import database

class Location():
    name = ""
    latitude = ""
    longitude = ""
    magnitude = ""
    bortle = ""
    brightness = ""
    artifical_brightness = ""

    def __init__(self, name:str, latitude:str, longitude:str):
        self.name = name
        self.latitude = latitude
        self.longitude = longitude

    def loadData(self):
        r = requests.get(f"https://clearoutside.com/forecast/{self.latitude}/{self.longitude}")
        if r.status_code != 200:
            print(f"ERROR status code: {r.status_code}")
            return

        cleaner_regex = re.compile('<.*?>')
        for line in r.text.splitlines():
            if "Bortle" in line:
                # <span class="btn btn-primary btn-bortle-5"><span class="glyphicon glyphicon-eye-open" style="top: 2px;"></span> &nbsp; Est. Sky Quality: &nbsp;<strong>19.58</strong> Magnitude. &nbsp;<strong>Class 5</strong> Bortle. &nbsp;<strong>1.58</strong> mcd/m<sup>2</sup> Brightness. &nbsp;<strong>1412.91</strong> μcd/m<sup>2</sup> Artificial Brightness.</span>
                cleanline = re.sub(cleaner_regex, '', line).replace("&nbsp;", "")
                #   Est. Sky Quality: 19.58 Magnitude. Class 5 Bortle. 1.58 mcd/m2 Brightness. 1412.91 μcd/m2 Artificial Brightness.
                m = re.match(".*Sky Quality: ([.0-9]*) Magnitude.*Class ([0-9]*) Bortle\. ([.0-9]*) mcd/m2 Brightness\. ([.0-9]*) μcd/m2 Artificial", cleanline)
                self.magnitude = m.groups()[0]
                self.bortle = m.groups()[1]
                self.brightness = m.groups()[2]
                self.artifical_brightness = m.groups()[3]
    
    def location_upsert_stmt(self):
        insert_values = {
            "name": self.name,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "magnitude": self.magnitude,
            "bortle": self.bortle,
            "brightness_mcd_m2": self.brightness,
            "artifical_brightness_ucd_m2": self.artifical_brightness,
        }
        update_values = {
            "name": self.name,
            "magnitude": self.magnitude,
            "bortle": self.bortle,
            "brightness_mcd_m2": self.brightness,
            "artifical_brightness_ucd_m2": self.artifical_brightness,
        }
        # using the database object just for statement generation.. (HACK)
        d = database.Database("")
        return d.upsert_stmt(
            table="location",
            insert_values=insert_values,
            update_values=update_values,
            conflictColumns=['latitude', 'longitude'],
        )
      
class LocationControl():
    locations = [
        Location("RL", "35.6", "-78.8"),
        Location("BW", "35.8", "-79.0"),
        Location("3BA", "36.1", "-78.7"),
        Location("SRSP", "36.7", "-78.7"),
        Location("HW", "35.4", "-78.3"),
        Location("KDDS", "39.6", "-104.0"),
    ]

    def __init__(self):
        pass

    def loadAllData(self):
        for l in self.locations:
            l.loadData()
            print("Sleeping for 15 seconds.  The site is rate limited.")
            time.sleep(15)
    
    def location_upsert_stmts(self):
        output = []
        for l in self.locations:
            output.append(l.location_upsert_stmt())
        return output


if __name__ == '__main__':
    d = database.Database(common.DATABASE_ASTROPHOTGRAPHY)
    try:
        l = LocationControl()
        l.loadAllData()
        d.open()
        for upsert in l.location_upsert_stmts():
            print(upsert)
            d.execute(upsert)
        d.commit()
    except Exception as e:
        print(e)
        traceback.print_exc()
    finally:
        d.close()