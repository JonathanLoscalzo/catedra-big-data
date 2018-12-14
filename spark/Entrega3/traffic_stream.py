import random
import time

SIMULATION_DELAY = 0.2
LATITUDE = 100
LONGITUDE = 100
MAX_VEL = 10
MAX_WAIT = 50
CANT_VEHICLES = 20
PLACE_PROB = 0.7
PLACES = ["Zoologico", "Shopping", "Plaza", "Museo", "Cine", "Teatro"]
OTHER_PLACE = "Otro"
CANT_PLACES = 50


class Vehicle:
    
    def __init__(self, _id, _places):
        self.places = _places
        self.id = _id
        self.lat = random.randint(1, LATITUDE)
        self.lon = random.randint(1, LONGITUDE)
        self.vel = random.randint(1, MAX_VEL)
        self.wait = random.randint(0, MAX_WAIT)
        self.place = ""
        self.move = MAX_VEL
        
        (self.des_lat, self.des_lon, self.des_place) = self.nextPlace()
        
    def step(self):
        if(self.wait > 0):
            self.wait = self.wait - 1
            self.place = ""
        else:
            self.move = self.move - self.vel
            if(self.move <= 0):
                if(self.lat > self.des_lat):
                    self.lat = self.lat - 1
                elif(self.lat < self.des_lat):
                    self.lat = self.lat + 1
                elif(self.lon < self.des_lon):
                    self.lon = self.lon + 1
                elif(self.lon > self.des_lon):
                    self.lon = self.lon - 1
                else:
                    self.wait = random.randint(1, MAX_WAIT)
                    self.place = self.des_place                    
                    (self.des_lat, self.des_lon, self.des_place) = self.nextPlace()
                    
                self.move = MAX_VEL + self.move
                return True
        return False
                
    def nextPlace(self):
        p = random.random()
        if(p < PLACE_PROB):
            p = random.randint(0, len(self.places)-1)
            return self.places[p]
        else:
            return (random.randint(1, LATITUDE), random.randint(1, LONGITUDE), OTHER_PLACE)
        
places = []
for i in range(CANT_PLACES):
    place = random.randint(0, len(PLACES)-1)
    places.append((random.randint(1, LATITUDE), random.randint(1, LONGITUDE), PLACES[place]))
        
vehicles = []
for i in range(CANT_VEHICLES):
    v = Vehicle(i+1, places)
    vehicles.append(v)

cur_time = 0
while True:
    for v in vehicles:
        if v.step():
            print(str(v.id) + ";" + str(v.lat) + ";" + str(v.lon) + ";" + str(cur_time) + ";" + v.place)
    cur_time = cur_time + 1
    time.sleep(SIMULATION_DELAY)
